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

#include <utility>

#include <fmt/ranges.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include <thrift/lib/cpp/concurrency/Thread.h>
#include <thrift/lib/cpp2/server/RoundRobinRequestPile.h>

namespace apache::thrift {

RoundRobinRequestPileOptions::RoundRobinRequestPileOptions() {
  numBucketsPerPriority.reserve(kDefaultNumPriorities);
  for (unsigned i = 0; i < kDefaultNumPriorities; ++i) {
    numBucketsPerPriority.emplace_back(kDefaultNumBuckets);
  }
}

void RoundRobinRequestPileOptions::setPreEnqueueFilter(PreEnqueueFilter fn) {
  preEnqueueFilter = std::move(fn);
}

void RoundRobinRequestPileOptions::setName(std::string rName) {
  name = std::move(rName);
}

void RoundRobinRequestPileOptions::setNumPriorities(unsigned int numPri) {
  numBucketsPerPriority.clear();
  numBucketsPerPriority.resize(numPri, kDefaultNumBuckets);
}

void RoundRobinRequestPileOptions::setNumBucketsPerPriority(
    uint32_t priority, unsigned int numBucket) {
  numBucketsPerPriority.at(priority) = numBucket;
}

void RoundRobinRequestPileOptions::setShape(std::vector<unsigned int> shape) {
  numBucketsPerPriority = std::move(shape);
}

void RoundRobinRequestPileOptions::setPileSelectionFunction(
    PileSelectionFunction func) {
  pileSelectionFunction = std::move(func);
}

void RoundRobinRequestPileOptions::setNumMaxRequestsPerPriority(
    std::vector<uint32_t> limits) {
  numMaxRequestsPerPriority = std::move(limits);
}

void RoundRobinRequestPileOptions::setNumMaxRequests(uint64_t limit) {
  numMaxRequests = limit;
}

const std::string& RoundRobinRequestPileOptions::getName() const {
  return name;
}

const std::vector<uint32_t>&
RoundRobinRequestPileOptions::getNumBucketsPerPriority() const {
  return numBucketsPerPriority;
}

uint64_t RoundRobinRequestPileOptions::getNumMaxRequests() const {
  return numMaxRequests;
}

const std::vector<uint32_t>&
RoundRobinRequestPileOptions::getNumMaxRequestsPerPriority() const {
  return numMaxRequestsPerPriority;
}

const RoundRobinRequestPileOptions::PileSelectionFunction&
RoundRobinRequestPileOptions::getPileSelectionFunction() const {
  return pileSelectionFunction;
}

const RoundRobinRequestPileOptions::PreEnqueueFilter&
RoundRobinRequestPileOptions::getPreEnqueueFilter() const {
  return preEnqueueFilter;
}

uint32_t RoundRobinRequestPileOptions::getNumMaxRequestsForPriority(
    unsigned priority) const {
  if (!numMaxRequestsPerPriority.empty()) {
    return numMaxRequestsPerPriority.at(priority);
  }
  return numMaxRequests;
}

std::string RoundRobinRequestPileOptions::describe() const {
  return fmt::format(
      "{{Options name={} numBucketsPerPriority={{{}}} numMaxRequests={} numMaxRequestsPerPriority={{{}}}}}",
      name,
      fmt::join(numBucketsPerPriority, ","),
      numMaxRequests,
      fmt::join(numMaxRequestsPerPriority, ","));
}

RoundRobinRequestPileOptions::PileSelectionFunction
RoundRobinRequestPileOptions::getDefaultPileSelectionFunc(
    unsigned defaultPriority) {
  DCHECK(numBucketsPerPriority.size());
  unsigned priorityLimit = numBucketsPerPriority.size() - 1;
  return [defaultPriority, priorityLimit](
             const ServerRequest& request) -> std::pair<unsigned, unsigned> {
    unsigned priority =
        static_cast<unsigned>(request.requestContext()->getCallPriority());
    if (priority <= priorityLimit) {
      return std::make_pair(static_cast<uint64_t>(priority), 0);
    }
    if (request.methodMetadata()) {
      if (auto prio = request.methodMetadata()->priority) {
        priority = static_cast<unsigned>(*prio);
      } else {
        priority = defaultPriority;
      }

    } else {
      priority = defaultPriority;
    }
    return std::make_pair(static_cast<uint64_t>(priority), 0);
  };
}

folly::AtomicNotificationQueueTaskStatus
RoundRobinRequestPile::Consumer::operator()(
    ServerRequest&& request, std::shared_ptr<folly::RequestContext>&&) {
  // If Executor for expiring requests is NOT provided, we return even expired
  // requests to the user
  if (requestExpirationDelegate_ == nullptr) {
    carrier_ = std::move(request);
    return folly::AtomicNotificationQueueTaskStatus::CONSUMED;
  }

  // If Executor to expiring requests IS provided, we schedule expiration of
  // expired requests on that Executor and return only not-yet-expired requests
  // to the user

  using namespace apache::thrift::detail;

  if (request.request() != nullptr && !request.request()->isActive()) {
    // NOTE that if request()->isActive() returns false we know *for sure* that
    // request has expired, but if it returns true then request MIGHT be
    // expired or not.
    if (dequeueObserver_) {
      (*dequeueObserver_)(request);
    }
    requestExpirationDelegate_->processExpiredRequest(std::move(request));
    return folly::AtomicNotificationQueueTaskStatus::DISCARD;
  }

  carrier_ = std::move(request);
  return folly::AtomicNotificationQueueTaskStatus::CONSUMED;
}

RoundRobinRequestPile::RoundRobinRequestPile(Options opts)
    : RequestPileBase(std::string(opts.getName())) {
  opts_ = std::move(opts);
  const auto& numBucketsPerPriority = opts_.getNumBucketsPerPriority();

  requestQueues_ = std::make_unique<std::unique_ptr<RequestQueue[]>[]>(
      numBucketsPerPriority.size());
  retrievalIndexQueues_ =
      std::make_unique<std::optional<RetrievalIndexQueue>[]>(
          numBucketsPerPriority.size());
  singleBucketRequestQueues_ =
      std::make_unique<std::unique_ptr<SingleBucketRequestQueue>[]>(
          numBucketsPerPriority.size());

  DCHECK(
      opts_.getNumMaxRequestsPerPriority().empty() ||
      opts_.getNumMaxRequestsPerPriority().size() ==
          numBucketsPerPriority.size());

  for (unsigned i = 0; i < numBucketsPerPriority.size(); ++i) {
    DCHECK(numBucketsPerPriority.at(i));

    singleBucketRequestQueues_[i] = std::make_unique<SingleBucketRequestQueue>(
        opts_.getNumMaxRequestsForPriority(i));

    requestQueues_[i] =
        std::make_unique<RequestQueue[]>(numBucketsPerPriority.at(i));

    if (numBucketsPerPriority.at(i) == 1) {
      // explicitly set it to null
      retrievalIndexQueues_[i] = std::nullopt;
    } else {
      // copy constructor is deleted so we use placement new
      new (&retrievalIndexQueues_[i])
          std::optional<RetrievalIndexQueue>(std::in_place);
    }

    for (unsigned j = 0; j < numBucketsPerPriority.at(i); ++j) {
      // because we are doing round robin
      // for each round we only want to consumer
      // one item from the queue
      requestQueues_[i][j].setMaxReadAtOnce(1);
      requestQueues_[i][j].arm();
    }
  }
}

std::optional<ServerRequest> RoundRobinRequestPile::dequeueImpl(
    unsigned pri, unsigned bucket) {
  Consumer consumer(
      requestExpirationDelegate_,
      dequeueObserver_ ? &dequeueObserver_ : nullptr);
  auto& queue = requestQueues_[pri][bucket];
  queue.drive(consumer);

  // Schedule a callback from the consumer thread iff
  // there is something remaining in the queue
  // we only store the index of next queue
  if (!queue.arm()) {
    retrievalIndexQueues_[pri]->enqueue(bucket);
  }

  if (consumer.carrier_) {
    RequestPileBase::onDequeued(*consumer.carrier_);
    if (dequeueObserver_) {
      dequeueObserver_(*consumer.carrier_);
    }
    return std::move(*consumer.carrier_);
  } else {
    return std::nullopt;
  }
}

std::optional<ServerRequestRejection> RoundRobinRequestPile::enqueue(
    ServerRequest&& request) {
  // record the enqueue time
  auto& info = request.requestData();
  info.queueBegin = std::chrono::steady_clock::now();

  unsigned pri = 0, bucket = 0;
  if (opts_.getPileSelectionFunction()) {
    std::tie(pri, bucket) = opts_.getPileSelectionFunction()(request);
  }
  DCHECK_LT(pri, opts_.getNumBucketsPerPriority().size());
  DCHECK_LT(bucket, opts_.getNumBucketsPerPriority()[pri]);
  request.requestData().bucket = {pri, bucket};
  RequestPileBase::onEnqueued(request);

  // Check pre-enqueue filter before actual enqueue
  if (opts_.getPreEnqueueFilter()) {
    if (auto rejection = opts_.getPreEnqueueFilter()(request)) {
      if (onRequestRejectedFunction_) {
        onRequestRejectedFunction_(pri, bucket);
      }
      return rejection;
    }
  }

  // Read before `request` is moved into a queue.
  const size_t wiredRequestBytes =
      limitRejectionObserver_ && request.requestContext()
      ? request.requestContext()->getWiredRequestBytes()
      : 0;

  // TODO(yichengfb): enforcing limit on single bucket queue
  // We temporarily disable limit for single bucket queue
  // because we currently rely on AtomicNotificationQueue to
  // achieve that.
  if (!retrievalIndexQueues_[pri]) {
    if (singleBucketRequestQueues_[pri]->enqueue(std::move(request))) {
      if (onRequestAcceptedFunction_) {
        onRequestAcceptedFunction_(pri, bucket);
      }
      return std::nullopt;
    }
    if (onRequestRejectedFunction_) {
      onRequestRejectedFunction_(pri, bucket);
    }
    if (limitRejectionObserver_) {
      limitRejectionObserver_(wiredRequestBytes);
    }
    return std::make_optional<ServerRequestRejection>(AppServerException(
        "AppServerException", "RequestPile enqueue error : reached max limit"));
  }

  if (auto maxRequest = opts_.getNumMaxRequestsForPriority(pri)) {
    auto res =
        requestQueues_[pri][bucket].tryPush(std::move(request), maxRequest);
    switch (res) {
      case RequestQueue::TryPushResult::FAILED_LIMIT_REACHED: {
        if (onRequestRejectedFunction_) {
          onRequestRejectedFunction_(pri, bucket);
        }
        if (limitRejectionObserver_) {
          limitRejectionObserver_(wiredRequestBytes);
        }
        return std::make_optional<ServerRequestRejection>(AppServerException(
            "AppServerException",
            "RequestPile enqueue error : reached max limit"));
      }
      case RequestQueue::TryPushResult::SUCCESS_AND_ARMED: {
        retrievalIndexQueues_[pri]->enqueue(bucket);
        break;
      }
      default:
        break;
    }
  } else {
    if (requestQueues_[pri][bucket].push(std::move(request))) {
      // Schedule a pull iff we are the first one to push into
      // the request queue
      retrievalIndexQueues_[pri]->enqueue(bucket);
    }
  }

  if (onRequestAcceptedFunction_) {
    onRequestAcceptedFunction_(pri, bucket);
  }
  return std::nullopt;
}

std::optional<ServerRequest> RoundRobinRequestPile::dequeue() {
  for (unsigned i = 0; i < opts_.getNumBucketsPerPriority().size(); ++i) {
    if (!retrievalIndexQueues_[i]) {
      if (auto req = singleBucketRequestQueues_[i]->tryDequeue()) {
        if (dequeueObserver_) {
          dequeueObserver_(*req);
        }
        return std::move(*req);
      }
    } else {
      if (auto bucket = retrievalIndexQueues_[i]->try_dequeue()) {
        auto result = dequeueImpl(i, *bucket);
        return result;
      }
    }
  }
  return std::nullopt;
}

uint64_t RoundRobinRequestPile::requestCount() const {
  uint64_t res = 0;
  for (size_t i = 0; i < opts_.getNumBucketsPerPriority().size(); ++i) {
    if (!retrievalIndexQueues_[i]) {
      res += singleBucketRequestQueues_[i]->size();
    } else if (!retrievalIndexQueues_[i]->empty()) {
      for (size_t j = 0; j < opts_.getNumBucketsPerPriority()[i]; ++j) {
        res += requestQueues_[i][j].size();
      }
    }
  }

  return res;
}

std::string RoundRobinRequestPile::describe() const {
  std::string result;
  result = fmt::format(
      "RoundRobinRequestPile priorities:{}",
      opts_.getNumBucketsPerPriority().size());
  for (size_t i = 0; i < opts_.getNumBucketsPerPriority().size(); ++i) {
    result += fmt::format(
        " Pri:{} Buckets:{}", i, opts_.getNumBucketsPerPriority()[i]);
  }
  return result;
}

serverdbginfo::RequestPileDbgInfo RoundRobinRequestPile::getDbgInfo() const {
  serverdbginfo::RequestPileDbgInfo info;
  info.name() = folly::demangle(typeid(*this));
  info.prioritiesCount() = opts_.getNumBucketsPerPriority().size();

  for (size_t i = 0; i < opts_.getNumBucketsPerPriority().size(); ++i) {
    info.bucketsPerPriority()->push_back(opts_.getNumBucketsPerPriority()[i]);
  }

  info.perBucketRequestLimit() = opts_.getNumMaxRequests();
  info.queuedRequestsCount() = requestCount();

  return info;
}

std::vector<std::vector<uint64_t>> RoundRobinRequestPile::getRequestCounts()
    const {
  std::vector<std::vector<uint64_t>> res;
  res.reserve(opts_.getNumBucketsPerPriority().size());
  for (size_t i = 0; i < opts_.getNumBucketsPerPriority().size(); ++i) {
    if (!retrievalIndexQueues_[i]) {
      res.emplace_back(1, singleBucketRequestQueues_[i]->size());
    } else {
      std::vector<uint64_t> subRes;
      if (!retrievalIndexQueues_[i]->empty()) {
        subRes.reserve(opts_.getNumBucketsPerPriority()[i]);
        for (size_t j = 0; j < opts_.getNumBucketsPerPriority()[i]; ++j) {
          subRes.emplace_back(requestQueues_[i][j].size());
        }
      } else {
        subRes.resize(opts_.getNumBucketsPerPriority()[i]);
      }
      res.push_back(std::move(subRes));
    }
  }

  return res;
}

namespace {
/*
 * augmentWithInternalPriorities takes a PileSelectionFunction and updates it to
 * work with internal priorities. The updated PileSelectionFunction will return
 * a priority that is twice as large as the original priority, and will add 1 to
 * the priority if the internal priority is LO_PRI.
 *
 * The updated function will return the same bucket as the original.
 */
RoundRobinRequestPile::PileSelectionFunction augmentWithInternalPriorities(
    RoundRobinRequestPile::PileSelectionFunction&& func) {
  return [originalFunc = std::move(func)](const ServerRequest& req)
             -> std::pair<
                 RoundRobinRequestPile::Priority,
                 RoundRobinRequestPile::Bucket> {
    unsigned pri = 0, bucket = 0;
    if (originalFunc) {
      std::tie(pri, bucket) = originalFunc(req);
    }
    pri *= 2;
    if (detail::ServerRequestHelper::internalPriority(req) ==
        folly::Executor::LO_PRI) {
      pri += 1;
    }
    return {pri, bucket};
  };
}
} // namespace

RoundRobinRequestPileOptions
RoundRobinRequestPileOptions::addInternalPriorities() const {
  auto opts = *this;
  if (opts.numBucketsPerPriority.size() >
      std::numeric_limits<RoundRobinRequestPile::Priority>::max() / 2) {
    LOG(WARNING) << "Too many priorities, additional internal priorities "
                 << "will not be added.";
    return opts;
  }

  // Double the number of priorities
  std::vector<unsigned int> newNumBucketsPerPriority;
  newNumBucketsPerPriority.reserve(opts.numBucketsPerPriority.size() * 2);
  for (auto numBuckets : opts.numBucketsPerPriority) {
    // See comment on addInternalPriorities declaration for why we do this
    newNumBucketsPerPriority.push_back(numBuckets);
    newNumBucketsPerPriority.push_back(numBuckets);
  }
  opts.numBucketsPerPriority = std::move(newNumBucketsPerPriority);

  // Double the number of priorities also for per-priority limits.
  if (!opts.numMaxRequestsPerPriority.empty()) {
    std::vector<uint32_t> newNumMaxRequestsPerPriority;
    newNumMaxRequestsPerPriority.reserve(
        opts.numMaxRequestsPerPriority.size() * 2);
    for (auto limit : opts.numMaxRequestsPerPriority) {
      newNumMaxRequestsPerPriority.push_back(limit);
      newNumMaxRequestsPerPriority.push_back(limit);
    }
    opts.numMaxRequestsPerPriority = std::move(newNumMaxRequestsPerPriority);
  }

  // Update pileSelectionFunction
  opts.pileSelectionFunction =
      augmentWithInternalPriorities(std::move(opts.pileSelectionFunction));
  return opts;
}

} // namespace apache::thrift
