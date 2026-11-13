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

#include <folly/IntrusiveList.h>

namespace apache::thrift::fast_thrift::channel_pipeline {

/**
 * WriteReadyHook lets a handler get an onWriteReady() call when the pipeline
 * can take writes again.
 *
 * Embed it as a public member named `writeReadyHook_`. makeHandlerNode finds
 * it at compile time when the handler is added to the pipeline. The member
 * holds no state, because the pipeline records which handlers wait. This
 * struct will be removed.
 *
 * Result::Backpressure from fireWrite() means the message was accepted, and
 * the transport asks for no more for now. Do not keep that message to resend
 * it. It was moved into fireWrite(), so a resend would pass on an empty box.
 * Queue the messages that come after it instead, and send them from
 * onWriteReady().
 *
 * Call awaitWriteReady() after a fireWrite() that returned Backpressure, or
 * when the handler waits for a state of its own, such as a full send window.
 *
 * The transport starts a write-ready pass only after it returned Backpressure.
 * So a handler that waits for its own state must call
 * ctx.pipeline()->onWriteReady() itself when that state clears, or it may
 * never get onWriteReady().
 *
 * Usage:
 *   class MyHandler {
 *    public:
 *     WriteReadyHook writeReadyHook_;  // Detected by makeHandlerNode
 *     std::deque<TypeErasedBox> pending_;
 *
 *     Result onWrite(ContextImpl& ctx, TypeErasedBox&& msg) noexcept {
 *       if (ctx.isAwaitingWriteReady()) {
 *         pending_.push_back(std::move(msg));
 *         return Result::Backpressure;
 *       }
 *       const Result result = ctx.fireWrite(std::move(msg));
 *       if (result == Result::Backpressure) {
 *         ctx.awaitWriteReady();  // msg was accepted; hold back the next ones
 *       }
 *       return result;
 *     }
 *
 *     void onWriteReady(ContextImpl& ctx) noexcept {
 *       while (!pending_.empty()) {
 *         TypeErasedBox msg = std::move(pending_.front());
 *         pending_.pop_front();
 *         if (ctx.fireWrite(std::move(msg)) == Result::Backpressure) {
 *           return;  // Still registered for the next onWriteReady()
 *         }
 *       }
 *       ctx.cancelAwaitWriteReady();
 *     }
 *   };
 */
struct WriteReadyHook {};

/**
 * ReadReadyHook provides intrusive list membership for read-ready notification.
 *
 * Handlers that want to receive onReadReady() notifications embed this hook
 * as a public member named `readReadyHook_`.
 */
struct ReadReadyHook {
  folly::IntrusiveListHook hook;
  size_t handlerIndex{0}; // Set by PipelineImpl during initialization
  std::size_t lastNotifiedGeneration{0};
};

/**
 * List of handlers awaiting read ready notifications.
 * Maintained by PipelineImpl, walked when onReadReady() is called.
 */
using ReadReadyList = folly::IntrusiveList<ReadReadyHook, &ReadReadyHook::hook>;

} // namespace apache::thrift::fast_thrift::channel_pipeline
