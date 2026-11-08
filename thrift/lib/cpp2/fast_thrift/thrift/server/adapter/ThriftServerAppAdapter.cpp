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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerAppAdapter.h>

#include <algorithm>
#include <optional>
#include <utility>

#include <folly/io/async/Request.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/Cpp2BridgeExtension.h>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ResponsePayloads.h>

namespace apache::thrift::fast_thrift::thrift {

static_assert(
    ServerInboundAppAdapter<ThriftServerAppAdapter>,
    "ThriftServerAppAdapter must satisfy ServerInboundAppAdapter");
static_assert(
    ServerOutboundAppAdapter<ThriftServerAppAdapter>,
    "ThriftServerAppAdapter must satisfy ServerOutboundAppAdapter");
static_assert(
    ServerComposableAppAdapter<ThriftServerAppAdapter>,
    "ThriftServerAppAdapter must satisfy ServerComposableAppAdapter");

ThriftServerAppAdapter::~ThriftServerAppAdapter() {
  resetPipeline();
  auto cb = closeCallback_.withWLock([](auto& fn) { return std::move(fn); });
  if (cb) {
    cb();
  }
}

void ThriftServerAppAdapter::setCloseCallback(std::function<void()> cb) {
  closeCallback_.withWLock([&](auto& fn) { fn = std::move(cb); });
}

void ThriftServerAppAdapter::setPipeline(
    channel_pipeline::PipelineRef pipeline) noexcept {
  DCHECK(pipeline);
  pipeline_ = pipeline;
  pipelineGuard_ = pipeline_.guard();
  pipelineActive_ = true;
  evb_ = folly::getKeepAliveToken(pipeline_.eventBase());
}

void ThriftServerAppAdapter::resetPipeline() noexcept {
  folly::DelayedDestruction::DestructorGuard dg(this);
  pipelineActive_ = false;
  pipeline_.reset();
  pipelineGuard_.reset();
}

void ThriftServerAppAdapter::onConnectionClosed() noexcept {
  // Detach from pipeline. Stragglers (FastHandlerCallbacks still
  // alive, each holding a DG on us) call writeResponse, hit
  // pipelineActive_==false in writeResponseOnEventBase, and drop
  // silently. The pipeline is free to die — its DG count from us
  // just dropped.
  pipelineActive_ = false;
  pipelineGuard_.reset();
  fireCloseCallback();
}

void ThriftServerAppAdapter::fireCloseCallback() noexcept {
  auto cb = closeCallback_.withWLock([](auto& fn) { return std::move(fn); });
  if (cb && evb_) {
    evb_->runInEventBaseThread(std::move(cb));
  }
}

channel_pipeline::Result ThriftServerAppAdapter::dispatchRequest(
    channel_pipeline::TypeErasedBox&& msg) noexcept {
  auto& request = msg.get<ThriftServerRequestMessage>();
  DCHECK(request.streamId != 0) << "Invalid stream ID";

  const auto routing = getServerRequestRoutingMetadata(request);
  if (routing.status == ServerRequestRoutingStatus::MissingMetadata)
      [[unlikely]] {
    handleUnknownMethod(
        request.streamId, {}, std::move(request.requestContext));
    return channel_pipeline::Result::Success;
  }
  if (routing.status == ServerRequestRoutingStatus::UnsupportedRpcKind)
      [[unlikely]] {
    handleWrongRpcKind(
        request.streamId, routing.kind, std::move(request.requestContext));
    return channel_pipeline::Result::Success;
  }

  if (dispatchTable_) {
    if (auto dispatch = dispatchTable_->find(routing.methodName)) {
      return dispatch(this, std::move(msg));
    }
  } else if (localMethods_) {
    for (const auto& [name, process] : *localMethods_) {
      if (name == routing.methodName) {
        return dispatchRequestResponse(this, process, std::move(msg));
      }
    }
  }
  handleUnknownMethod(
      request.streamId, routing.methodName, std::move(request.requestContext));
  return channel_pipeline::Result::Success;
}

void ThriftServerAppAdapter::onException(
    folly::exception_wrapper&& e) noexcept {
  // Tail-of-pipeline contract: any exception reaching here is unhandled.
  // The pipeline is in a broken state — no point waiting for a graceful
  // drain (close() would). Tear it down immediately. PipelineImpl's
  // deactivate() is idempotent (state-gated), so racing with a
  // transport-initiated deactivate is safe — only the first call
  // cascades; the close handler's onPipelineInactive does the rest.
  XLOG_EVERY_MS(ERR, 60'000) << "Pipeline exception: " << e.what();
  if (pipeline_) {
    pipeline_.deactivate();
  }
}

void ThriftServerAppAdapter::addMethodHandler(
    std::string_view name, RequestResponseProcessFn handler) {
  if (dispatchTable_) {
    XLOG(DFATAL)
        << "Cannot add a method to an adapter backed by an immutable dispatch table";
    return;
  }
  if (!localMethods_) {
    localMethods_ = std::make_unique<
        std::vector<std::pair<std::string, RequestResponseProcessFn>>>();
  }
  for (auto& [existingName, existingHandler] : *localMethods_) {
    if (existingName == name) {
      existingHandler = handler;
      return;
    }
  }
  localMethods_->emplace_back(name, handler);
}

bool ThriftServerAppAdapter::hasMethod(std::string_view name) const noexcept {
  if (dispatchTable_) {
    return dispatchTable_->find(name) != nullptr;
  }
  return localMethods_ &&
      std::any_of(
             localMethods_->begin(),
             localMethods_->end(),
             [&](const auto& method) { return method.first == name; });
}

void ThriftServerAppAdapter::handleWrongRpcKind(
    uint32_t streamId,
    apache::thrift::RpcKind kind,
    ThriftRequestContextPtr requestContext) noexcept {
  XLOG(ERR) << "Unsupported RPC kind: " << static_cast<int>(kind);
  auto response = makeWrongRpcKindMessage(streamId, kind);
  response.requestContext = std::move(requestContext);
  writeResponseOnEventBase(std::move(response));
}

void ThriftServerAppAdapter::handleUnknownMethod(
    uint32_t streamId,
    std::string_view methodName,
    ThriftRequestContextPtr requestContext) noexcept {
  auto response = makeUnknownMethodMessage(streamId, methodName);
  response.requestContext = std::move(requestContext);
  writeResponseOnEventBase(std::move(response));
}

void ThriftServerAppAdapter::writeResponse(
    ThriftServerResponseMessage&& message,
    folly::DelayedDestruction::DestructorGuard&& adapterGuard) noexcept {
  if (evb_->isInEventBaseThread()) {
    writeResponseOnEventBase(std::move(message));
    return;
  }
  // adapterGuard is moved, not copied: a copy would construct a new guard on
  // this thread and mutate the non-atomic guardCount_ off the EventBase. The
  // move keeps us alive across the hop and releases on the EventBase.
  evb_->runInEventBaseThread([this,
                              adapterGuard = std::move(adapterGuard),
                              message = std::move(message)]() mutable {
    writeResponseOnEventBase(std::move(message));
  });
}

void ThriftServerAppAdapter::acknowledgeCancellation(
    uint32_t streamId,
    ThriftRequestContextPtr requestContext,
    folly::DelayedDestruction::DestructorGuard&& adapterGuard) noexcept {
  // Always defer, even when already on the EventBase. A cancellation-token
  // callback may acknowledge synchronously from inside
  // CancellationSource::requestCancellation(); destroying requestContext in
  // that stack would destroy the source while it is still notifying.
  evb_->runInEventBaseThread(
      [this,
       streamId,
       requestContext = std::move(requestContext),
       adapterGuard = std::move(adapterGuard)]() mutable {
        acknowledgeCancellationOnEventBase(streamId, std::move(requestContext));
      });
}

void ThriftServerAppAdapter::acknowledgeCancellationOnEventBase(
    uint32_t streamId, ThriftRequestContextPtr requestContext) noexcept {
  std::optional<server::RequestContextScopeGuard> requestContextGuard;
  if (requestContext != nullptr) {
    requestContextGuard.emplace(requestContext.get());
  }
  if (requestContext != nullptr && requestContext->tryComplete() &&
      pipelineActive_) {
    pipeline_.bindEvents<PublishedEvents>()
        .template fire<ThriftServerRequestCompletedEvent>(
            ThriftServerRequestCompletedEvent{.streamId = streamId});
  }
  requestContext.reset();
}

void ThriftServerAppAdapter::writeResponse(
    ThriftServerResponseMessage&& message) noexcept {
  DCHECK(evb_->isInEventBaseThread())
      << "writeResponse without a guard is EventBase-only; off-thread callers "
         "must donate a DestructorGuard so the adapter survives the hop";
  writeResponseOnEventBase(std::move(message));
}

void ThriftServerAppAdapter::writeResponseOnEventBase(
    ThriftServerResponseMessage&& message) noexcept {
  std::optional<server::RequestContextScopeGuard> requestContextGuard;
  if (message.requestContext != nullptr) {
    requestContextGuard.emplace(message.requestContext.get());
  }
  // Straggler after force-close (or any post-ConnectionClosed write):
  // the pipeline may already be gone. Drop silently — pipeline_ may
  // be dangling, do NOT dereference.
  if (!pipelineActive_) [[unlikely]] {
    message.requestContext.reset();
    return;
  }
  if (!pipeline_) [[unlikely]] {
    handleMissingPipeline();
    message.requestContext.reset();
    return;
  }
  const auto result =
      pipeline_.fireWrite(channel_pipeline::erase_and_box(std::move(message)));
  // A failed write means the response can't reach the wire — pipeline
  // is in a broken or torn-down state. Tear down immediately rather
  // than wait for a graceful drain. Idempotent at the pipeline level.
  if (result == channel_pipeline::Result::Error) [[unlikely]] {
    pipeline_.deactivate();
  }
}

void ThriftServerAppAdapter::handleMissingPipeline() noexcept {
  XLOG(ERR) << "Pipeline not set, cannot send response";
}

void ThriftServerAppAdapter::close() noexcept {
  if (!pipeline_) {
    return;
  }
  pipeline_.bindEvents<PublishedEvents>()
      .template fire<ThriftServerCloseConnectionEvent>(
          ThriftServerCloseConnectionEvent{});
}

} // namespace apache::thrift::fast_thrift::thrift
