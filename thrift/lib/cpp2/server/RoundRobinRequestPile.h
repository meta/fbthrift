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

#pragma once

#include <cstdint>
#include <limits>
#include <memory>

#include <folly/Executor.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <folly/io/async/AtomicNotificationQueue.h>

#include <thrift/lib/cpp2/async/AsyncProcessor.h>
#include <thrift/lib/cpp2/server/RequestExpirationDelegate.h>
#include <thrift/lib/cpp2/server/RequestPileBase.h>
#include <thrift/lib/cpp2/server/SingleBucketRequestPileQueue.h>

namespace apache::thrift {

class RoundRobinRequestPileOptions {
 public:
  using Priority = uint32_t;
  using Bucket = uint32_t;
  // Function to specify priority and bucket for a request. The priority must be
  // <= the number of priorities (lower numbers have higher priority). The
  // bucket must be <= the number of buckets available at the specified
  // priority.
  using PileSelectionFunction =
      std::function<std::pair<Priority, Bucket>(const ServerRequest&)>;

  // Pre-enqueue filter for custom rejection logic.
  // Called after priority is determined, before enqueue.
  // Return rejection to reject the request, nullopt to allow.
  using PreEnqueueFilter = std::function<std::optional<ServerRequestRejection>(
      const ServerRequest& request)>;

  inline constexpr static unsigned int kDefaultNumPriorities = 1;
  inline constexpr static unsigned int kDefaultNumBuckets = 1;

  RoundRobinRequestPileOptions();

  // Setters
  void setName(std::string rName);

  // Set the number of priority levels to provide. By default the number of
  // buckets per priority will be set to 1.
  void setNumPriorities(unsigned int numPri);

  void setNumBucketsPerPriority(Priority priority, unsigned int numBucket);

  // This function sets the shape of the RequestPile
  // e.g. This is setting the number of buckets per priority
  // If your RequestPile has 3 priorities and each has 2 buckets
  // you should pass in {2,2,2}
  void setShape(std::vector<unsigned int> shape);

  void setPileSelectionFunction(PileSelectionFunction func);

  // Set per-priority per-bucket limits. The vector size must match the
  // number of priorities. A value of 0 means no limit for that priority.
  void setNumMaxRequestsPerPriority(std::vector<uint32_t> limits);

  void setNumMaxRequests(uint64_t limit);

  void setPreEnqueueFilter(PreEnqueueFilter fn);

  // Getters
  const std::string& getName() const;
  const std::vector<uint32_t>& getNumBucketsPerPriority() const;
  uint64_t getNumMaxRequests() const;
  const std::vector<uint32_t>& getNumMaxRequestsPerPriority() const;
  const PileSelectionFunction& getPileSelectionFunction() const;
  const PreEnqueueFilter& getPreEnqueueFilter() const;

  // Get the effective per-bucket limit for a given priority.
  // Returns the per-priority limit if set, otherwise the global limit.
  uint32_t getNumMaxRequestsForPriority(unsigned priority) const;

  // emulating PriorityQueueThreadManager
  PileSelectionFunction getDefaultPileSelectionFunc(
      unsigned defaultPriority = static_cast<unsigned>(concurrency::NORMAL));

  std::string describe() const;

  /*
   * addInternalPriorities updates Options to work with internal priorities.
   * It does this by doubling the number of priorities while keeping the
   * number of buckets per priority the same. For example, if the original
   * shape was {1, 2} (high priority with 1 bucket and low priority with 2
   * buckets), the new shape will be {1, 1, 2, 2}. Requests with LO_PRI
   * internal priority will be placed in the second corresponding priority.
   * Internal priority is used by interactions to prioritize requests for
   * existing interactions over other requests.
   */
  RoundRobinRequestPileOptions addInternalPriorities() const;

 private:
  std::string name;

  // numBucket = numBuckets_[priority]
  std::vector<uint32_t> numBucketsPerPriority;

  // 0 means no limit
  // this is a per bucket limit
  uint64_t numMaxRequests{0};

  // Per-priority per-bucket limits. When set for a given priority,
  // overrides numMaxRequests for that priority. 0 means no limit.
  // If empty, numMaxRequests is used for all priorities.
  std::vector<uint32_t> numMaxRequestsPerPriority;

  // Function to route requests to priority/bucket
  PileSelectionFunction pileSelectionFunction;

  PreEnqueueFilter preEnqueueFilter;
};

class RoundRobinRequestPile : public RequestPileBase {
 public:
  using Options = RoundRobinRequestPileOptions;
  using Priority = Options::Priority;
  using Bucket = Options::Bucket;
  using PileSelectionFunction = Options::PileSelectionFunction;

  // The default number of buckets for each priority is 1
  explicit RoundRobinRequestPile(Options opts);

  ~RoundRobinRequestPile() override = default;

  std::optional<ServerRequestRejection> enqueue(
      ServerRequest&& request) override;

  std::optional<ServerRequest> dequeue() override;

  uint64_t requestCount() const override;

  // Return the current number of requests in the request
  // pile's queues per priority per bucket.
  std::vector<std::vector<uint64_t>> getRequestCounts() const;

  void onRequestFinished(ServerRequestData&) override {}

  void setRequestExpirationDelegate(
      RequestExpirationDelegate* requestExpirationDelegate) {
    requestExpirationDelegate_ = requestExpirationDelegate;
  }

  std::string describe() const override;

  serverdbginfo::RequestPileDbgInfo getDbgInfo() const override;

 private:
  // we choose 1KB as segment size to minimize allocations
  using RetrievalIndexQueue = folly::UMPMCQueue<
      unsigned,
      /* MayBlock  */ false,
      /* log2(SegmentSize=1024) */ 10>;

  // The reason why we chose AtomicNotificationQueue instead of two UMPMCQueue
  // is that UMPMCQueue does not provide an enqueue() interface that indicates
  // which one is the first enqueue to the empty queue
  using RequestQueue = folly::AtomicNotificationQueue<ServerRequest>;

  // This is a temporary solution to single bucket case
  // because ReqeustQueue is a MPSC queue
  using SingleBucketRequestQueue =
      server::SingleBucketRequestPileQueue<ServerRequest>;

  using BucketEventLoggingFunction = std::function<void(Priority, Bucket)>;
  using DequeueObserver = std::function<void(const ServerRequest& request)>;
  using LimitRejectionObserver = std::function<void(size_t wiredRequestBytes)>;

  // the consumer class used by the AtomicNotificationQueue
  class Consumer {
   public:
    explicit Consumer(
        RequestExpirationDelegate* requestExpirationDelegate = nullptr,
        DequeueObserver* dequeueObserver = nullptr)
        : requestExpirationDelegate_(requestExpirationDelegate),
          dequeueObserver_(dequeueObserver) {}

    // this operation simply put the retrieved item into the temporary
    // carrier for the caller to extract

    folly::AtomicNotificationQueueTaskStatus operator()(
        ServerRequest&& req, std::shared_ptr<folly::RequestContext>&&);

    RequestExpirationDelegate* requestExpirationDelegate_{nullptr};
    DequeueObserver* dequeueObserver_{nullptr};
    std::optional<ServerRequest> carrier_{std::nullopt};
  };

  Options opts_;
  // The reason why we chose unique_ptr managed array
  // instead of std containers like vector is that
  // the queues are not movable / move constructable
  // which is necessary for standard container
  std::unique_ptr<std::unique_ptr<RequestQueue[]>[]> requestQueues_;
  // we use std::nullopt to signal that a certain priority is
  // using only one bucket - in that case we don't need retrieval queue
  std::unique_ptr<std::optional<RetrievalIndexQueue>[]> retrievalIndexQueues_;

  std::unique_ptr<std::unique_ptr<SingleBucketRequestQueue>[]>
      singleBucketRequestQueues_;

  std::optional<ServerRequest> dequeueImpl(unsigned pri, unsigned bucket);

  RequestExpirationDelegate* requestExpirationDelegate_{nullptr};

  BucketEventLoggingFunction onRequestAcceptedFunction_;
  BucketEventLoggingFunction onRequestRejectedFunction_;
  DequeueObserver dequeueObserver_;
  LimitRejectionObserver limitRejectionObserver_;

 public:
  // The (Priority, Bucket) callbacks count every accepted or rejected request,
  // including rejections by the pre-enqueue filter.
  // The dequeue observer receives each queued request when it leaves the pile.
  // The limit-rejection observer receives the wired request bytes of a request
  // that passes the pre-enqueue filter but is rejected by the per-bucket limit.
  void setOnRequestAcceptedFunction(BucketEventLoggingFunction fn) {
    onRequestAcceptedFunction_ = std::move(fn);
  }

  void setOnRequestRejectedFunction(BucketEventLoggingFunction fn) {
    onRequestRejectedFunction_ = std::move(fn);
  }

  void setDequeueObserver(DequeueObserver fn) {
    dequeueObserver_ = std::move(fn);
  }

  void setLimitRejectionObserver(LimitRejectionObserver fn) {
    limitRejectionObserver_ = std::move(fn);
  }
};

// RoundRobinRequestPile is the default request pile used by most of
// the thrift servers
using StandardRequestPile = RoundRobinRequestPile;

} // namespace apache::thrift
