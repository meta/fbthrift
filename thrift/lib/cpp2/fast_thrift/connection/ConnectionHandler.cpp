/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <thrift/lib/cpp2/fast_thrift/connection/ConnectionHandler.h>

#include <utility>
#include <vector>

namespace apache::thrift::fast_thrift::connection {

ConnectionHandler::ConnectionHandler(
    folly::EventBase& evb,
    fast_security::SSLPolicy sslPolicy,
    folly::observer::Observer<std::shared_ptr<const fast_security::TLSParams>>
        tlsParamsObserver,
    SocketOptions socketOptions,
    std::optional<folly::observer::Observer<uint32_t>>
        maxConnectionsPerIOThread,
    ConnectionStats* stats,
    security::TLSStats* tlsStats)
    : evb_(folly::getKeepAliveToken(&evb)),
      sslPolicy_(sslPolicy),
      tlsParamsObserver_(std::move(tlsParamsObserver)),
      socketOptions_(std::move(socketOptions)),
      maxConnectionsPerIOThread_(std::move(maxConnectionsPerIOThread)),
      listener_(evb, socketOptions_) {
  // Resolved here rather than at pipeline-build time because this ctor runs
  // on the EventBase that will own every connection this handler accepts —
  // making it the thread whose shard these counts belong in, and the only
  // thread that will ever write it.
  if (stats != nullptr) {
    DCHECK(evb_->isInEventBaseThread());
    statsShard_ = &stats->currentThreadShard();
  }
  if (tlsStats != nullptr) {
    DCHECK(evb_->isInEventBaseThread());
    tlsShard_ = &tlsStats->currentThreadShard();
  }
}

ConnectionHandler::~ConnectionHandler() {
  // Defensive: ensure shutdown runs at least once. From the EVB we can't
  // wait for graceful drain (would block the loop that fires close
  // callbacks), so fall back to a synchronous teardown — same as the
  // original behavior of this dtor.
  if (evb_->inRunningEventBaseThread()) {
    stopInstallingOnEvb();
    closeAllConnectionsOnEvb();
  } else {
    stop();
  }
}

void ConnectionHandler::stop() {
  DCHECK(!evb_->inRunningEventBaseThread())
      << "ConnectionHandler::stop must not be called from the owning EVB; "
      << "the wait would block the loop that fires close callbacks";

  // Phase 1: reject new connection installs.
  evb_->runImmediatelyOrRunInEventBaseThreadAndWait(
      [this] { stopInstallingOnEvb(); });

  // Phase 2: trigger close on every live connection.
  evb_->runImmediatelyOrRunInEventBaseThreadAndWait(
      [this] { closeAllOnEvb(); });

  // Phase 3: wait for every connection to fully tear down. Close callbacks
  // decrement connections_ and post drainedBaton_ when the last one drops.
  // Each connection is responsible for bounding its own termination (the
  // thrift connection's close handler owns drain + reap deadlines and
  // LOG(FATAL)s on stuck callbacks), so no outer timeout is needed here.
  // Off-EVB so the loop is free to fire those callbacks.
  drainedBaton_.wait();
}

void ConnectionHandler::stopInstallingOnEvb() {
  if (!pipeline_) {
    return;
  }
  pipeline_->deactivate();
  listener_.resetPipeline();
  pipeline_.reset();
}

void ConnectionHandler::closeAllOnEvb() {
  draining_.store(true, std::memory_order_release);
  if (connections_.empty()) {
    postDrainedOnce();
    return;
  }
  // Snapshot keys so iteration is safe across re-entrant erases (close()
  // may fire the close callback synchronously for connection types
  // without async teardown work).
  std::vector<uint64_t> ids;
  ids.reserve(connections_.size());
  for (const auto& [id, _] : connections_) {
    ids.push_back(id);
  }
  for (auto id : ids) {
    auto it = connections_.find(id);
    if (it != connections_.end()) {
      it->second.close(/*notifyOwner=*/true);
    }
  }
}

void ConnectionHandler::closeAllConnectionsOnEvb() {
  // Move the map out before tearing down so destroying each connection cannot
  // invalidate the iteration.
  auto victims = std::move(connections_);
  // Emergency teardown detaches each owner callback before close, so update
  // the bookkeeping directly.
  connectionCount_.store(0, std::memory_order_relaxed);
  // Same reasoning for the gauge, which the no-op erase would likewise skip.
  // Subtracted rather than zeroed: the shard is per-EventBase and shared with
  // every other handler on this thread, so it is not this handler's to reset.
  if (statsShard_ != nullptr) {
    statsShard_->connectionsActive.incrementValue(
        -static_cast<int64_t>(victims.size()));
  }
  for (auto& [_, conn] : victims) {
    conn.close(/*notifyOwner=*/false);
  }
}

void ConnectionHandler::onConnectionClosed(uint64_t connId) noexcept {
  if (connections_.erase(connId) > 0) {
    connectionCount_.fetch_sub(1, std::memory_order_relaxed);
    // Paired with the increment in the acceptance pipeline's tail metrics
    // handler. Decremented here, under the same erase that keeps
    // connectionCount_ honest, so the gauge cannot drift from the map: every
    // connection counted in was registered, and every registered connection
    // is erased exactly once.
    if (statsShard_ != nullptr) {
      statsShard_->connectionsActive.incrementValue(-1);
    }
  }
  if (draining_.load(std::memory_order_acquire) && connections_.empty()) {
    postDrainedOnce();
  }
}

void ConnectionHandler::postDrainedOnce() noexcept {
  bool expected = false;
  if (drainedPosted_.compare_exchange_strong(expected, true)) {
    drainedBaton_.post();
  }
}

} // namespace apache::thrift::fast_thrift::connection
