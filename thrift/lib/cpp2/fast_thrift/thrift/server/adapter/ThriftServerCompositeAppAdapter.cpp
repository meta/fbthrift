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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerCompositeAppAdapter.h>

#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <folly/io/async/Request.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/Cpp2BridgeExtension.h>

#include <thrift/lib/cpp2/fast_thrift/thrift/common/ThriftResponsePayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ResponsePayloads.h>

namespace apache::thrift::fast_thrift::thrift {

ThriftServerCompositeAppAdapter::~ThriftServerCompositeAppAdapter() {
  resetPipeline();
  // Fallback: if onPipelineInactive never ran (e.g. composite destroyed
  // without prior pipeline tear-down), fire the close callback synchronously.
  auto cb = closeCallback_.withWLock([](auto& fn) { return std::move(fn); });
  if (cb) {
    cb();
  }
}

void ThriftServerCompositeAppAdapter::setCloseCallback(
    std::function<void()> cb) {
  closeCallback_.withWLock([&](auto& fn) { fn = std::move(cb); });
}

channel_pipeline::Result ThriftServerCompositeAppAdapter::onReadImpl(
    channel_pipeline::TypeErasedBox&& msg) noexcept {
  auto& request = msg.get<ThriftServerRequestMessage>();
  DCHECK(request.streamId != 0) << "Invalid stream ID";

  const auto routing = getServerRequestRoutingMetadata(request);
  if (routing.status == ServerRequestRoutingStatus::MissingMetadata)
      [[unlikely]] {
    return writeUnknownMethodError(
        request.streamId, {}, std::move(request.requestContext));
  }
  if (routing.status == ServerRequestRoutingStatus::UnsupportedRpcKind)
      [[unlikely]] {
    return writeWrongRpcKindError(
        request.streamId, routing.kind, std::move(request.requestContext));
  }

  if (routingTable_) {
    const auto* route = routingTable_->find(routing.methodName);
    if (route == nullptr) [[unlikely]] {
      return writeUnknownMethodError(
          request.streamId,
          routing.methodName,
          std::move(request.requestContext));
    }
    DCHECK_LT(route->childIndex, children_.size());
    return route->dispatch(
        children_[route->childIndex].adapter, std::move(msg));
  }

  for (const auto& child : children_) {
    if (child.adapter->hasMethod(routing.methodName)) {
      return child.adapter->dispatchRequest(std::move(msg));
    }
  }
  return writeUnknownMethodError(
      request.streamId, routing.methodName, std::move(request.requestContext));
}

void ThriftServerCompositeAppAdapter::onException(
    folly::exception_wrapper&& e) noexcept {
  // Tail-of-pipeline contract: any exception reaching here is unhandled.
  // Tear down the pipeline immediately (no graceful drain). Children
  // also get the notification for any per-child cleanup, but the
  // pipeline teardown is owned here; their own onException → deactivate
  // calls are absorbed by PipelineImpl::deactivate's idempotency.
  XLOG_EVERY_MS(ERR, 60'000) << "Pipeline exception: " << e.what();
  for (auto& child : children_) {
    child.vtable->onException(child.owner, folly::exception_wrapper{e});
  }
  if (pipeline_) {
    pipeline_.deactivate();
  }
}

void ThriftServerCompositeAppAdapter::setPipeline(
    channel_pipeline::PipelineRef pipeline) noexcept {
  pipeline_ = pipeline;
  pipelineGuard_ = pipeline_.guard();
  for (auto& child : children_) {
    child.vtable->setPipeline(child.owner, pipeline);
  }
}

void ThriftServerCompositeAppAdapter::resetPipeline() noexcept {
  folly::DelayedDestruction::DestructorGuard dg(this);
  // Mirror setPipeline's fan-out: each child holds its own pipelineGuard_
  // taken when we forwarded setPipeline to them. Release theirs first so
  // their later dtors don't fire pipeline-dtor cascades.
  for (auto& child : children_) {
    child.vtable->resetPipeline(child.owner);
  }
  pipeline_.reset();
  pipelineGuard_.reset();
}

void ThriftServerCompositeAppAdapter::handlerAdded() noexcept {
  for (auto& child : children_) {
    child.vtable->handlerAdded(child.owner);
  }
}

void ThriftServerCompositeAppAdapter::handlerRemoved() noexcept {
  for (auto& child : children_) {
    child.vtable->handlerRemoved(child.owner);
  }
}

void ThriftServerCompositeAppAdapter::onPipelineActive() noexcept {
  for (auto& child : children_) {
    child.vtable->onPipelineActive(child.owner);
  }
}

void ThriftServerCompositeAppAdapter::onPipelineInactive() noexcept {
  // Fan out to children only. The user closeCallback is NOT fired here
  // — that fires on the ConnectionClosed broadcast from the
  // ThriftServerConnectionCloseHandler, which is the edge that guarantees
  // in-flight handler callbacks have settled (or LOG(FATAL)'d on reap timeout).
  for (auto& child : children_) {
    child.vtable->onPipelineInactive(child.owner);
  }
}

void ThriftServerCompositeAppAdapter::onConnectionClosed() noexcept {
  // Deferred onto the EventBase because the callback's typical action
  // is to destroy *us*, and we're called from inside a pipeline
  // event-dispatch walk. Skip the move-out when pipeline_ is null so
  // the dtor fallback still has the callback to fire.
  if (!pipeline_) {
    return;
  }
  auto cb = closeCallback_.withWLock([](auto& fn) { return std::move(fn); });
  if (cb) {
    pipeline_.eventBase()->runInEventBaseThread(std::move(cb));
  }
}

void ThriftServerCompositeAppAdapter::onWriteReady() noexcept {
  for (auto& child : children_) {
    child.vtable->onWriteReady(child.owner);
  }
}

channel_pipeline::Result
ThriftServerCompositeAppAdapter::writeUnknownMethodError(
    uint32_t streamId,
    std::string_view methodName,
    ThriftRequestContextPtr requestContext) noexcept {
  return writeFrameworkError(
      makeUnknownMethodMessage(streamId, methodName),
      std::move(requestContext));
}

channel_pipeline::Result
ThriftServerCompositeAppAdapter::writeWrongRpcKindError(
    uint32_t streamId,
    apache::thrift::RpcKind kind,
    ThriftRequestContextPtr requestContext) noexcept {
  XLOG_EVERY_MS(ERR, 60'000)
      << "Unsupported RPC kind: " << static_cast<int>(kind);
  return writeFrameworkError(
      makeWrongRpcKindMessage(streamId, kind), std::move(requestContext));
}

channel_pipeline::Result ThriftServerCompositeAppAdapter::writeFrameworkError(
    ThriftServerResponseMessage&& message,
    ThriftRequestContextPtr requestContext) noexcept {
  message.requestContext = std::move(requestContext);
  std::optional<server::RequestContextScopeGuard> requestContextGuard;
  if (message.requestContext != nullptr) {
    requestContextGuard.emplace(message.requestContext.get());
  }
  if (!pipeline_) [[unlikely]] {
    XLOG(ERR) << "Pipeline not set, cannot send framework error";
    message.requestContext.reset();
    return channel_pipeline::Result::Error;
  }
  auto result =
      pipeline_.fireWrite(channel_pipeline::erase_and_box(std::move(message)));
  if (result == channel_pipeline::Result::Error) [[unlikely]] {
    pipeline_.deactivate();
  }
  return result;
}

void ThriftServerCompositeAppAdapter::close() noexcept {
  if (!pipeline_) [[unlikely]] {
    return;
  }
  pipeline_.bindEvents<PublishedEvents>()
      .template fire<ThriftServerCloseConnectionEvent>(
          ThriftServerCloseConnectionEvent{});
}

} // namespace apache::thrift::fast_thrift::thrift
