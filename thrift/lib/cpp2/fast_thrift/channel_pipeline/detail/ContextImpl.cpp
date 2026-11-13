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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineImpl.h>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

ContextImpl::ContextImpl(
    PipelineImpl* pipeline,
    folly::EventBase* eventBase,
    void* allocator,
    size_t handlerIndex,
    HandlerId handlerId) noexcept
    : pipeline_(pipeline),
      eventBase_(eventBase),
      allocator_(allocator),
      handlerIndex_(handlerIndex),
      handlerId_(handlerId) {}

void ContextImpl::activate() noexcept {
  // onPipelineActive is propagated via PipelineImpl::activate() which
  // iterates through all handlers. No-op here as handlers don't chain
  // activation.
}

void ContextImpl::deactivate() noexcept {
  if (handlerIndex_ < pipeline_->handlerCount()) {
    pipeline_->deactivateFromIndex(handlerIndex_);
  } else {
    pipeline_->deactivate();
  }
}

void ContextImpl::fireTypeEvent(EventKey key, const void* payload) noexcept {
  pipeline_->fireTypeEvent(key, payload);
}

void ContextImpl::fireTypeEventFromRoute(
    std::size_t routeIndex, EventKey key, const void* payload) noexcept {
  pipeline_->fireTypeEventFromRoute(
      typeEventRouteOffset_, routeIndex, key, payload);
}

BytesPtr ContextImpl::allocate(size_t size) noexcept {
  return pipeline_->allocate(size);
}

BytesPtr ContextImpl::copyBuffer(const void* data, size_t size) noexcept {
  return pipeline_->copyBuffer(data, size);
}

BytesPtr ContextImpl::takeOwnership(OwnedBuffer buffer) noexcept {
  return pipeline_->takeOwnership(buffer);
}

void ContextImpl::close() noexcept {
  pipeline_->close();
}

void ContextImpl::awaitWriteReady() noexcept {
  if (!pipeline_->isClosed() && pipeline_->hasWriteReadyHook(handlerIndex_)) {
    pipeline_->writeReadyWaiters().arm(handlerIndex_);
  }
}

void ContextImpl::cancelAwaitWriteReady() noexcept {
  if (pipeline_->hasWriteReadyHook(handlerIndex_)) {
    pipeline_->writeReadyWaiters().cancel(handlerIndex_);
  }
}

bool ContextImpl::isAwaitingWriteReady() const noexcept {
  return pipeline_->hasWriteReadyHook(handlerIndex_) &&
      pipeline_->writeReadyWaiters().isArmed(handlerIndex_);
}

void ContextImpl::awaitReadReady() noexcept {
  if (!pipeline_->isClosed() && pipeline_->hasReadReadyHook(handlerIndex_)) {
    pipeline_->readReadyWaiters().arm(handlerIndex_);
  }
}

void ContextImpl::cancelAwaitReadReady() noexcept {
  if (pipeline_->hasReadReadyHook(handlerIndex_)) {
    pipeline_->readReadyWaiters().cancel(handlerIndex_);
  }
}

bool ContextImpl::isAwaitingReadReady() const noexcept {
  return pipeline_->hasReadReadyHook(handlerIndex_) &&
      pipeline_->readReadyWaiters().isArmed(handlerIndex_);
}

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
