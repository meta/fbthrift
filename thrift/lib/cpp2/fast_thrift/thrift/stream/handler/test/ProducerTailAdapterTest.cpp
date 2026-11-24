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

#include <thrift/lib/cpp2/fast_thrift/thrift/stream/handler/ProducerTailAdapter.h>

#include <stdexcept>

#include <gtest/gtest.h>

#include <folly/ExceptionWrapper.h>
#include <folly/Portability.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift::stream {

namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::Result;
using channel_pipeline::TypeErasedBox;

// onRead ignores its context; a stub satisfies the signature.
struct StubContext {};

using Handler = ProducerTailAdapter;

TypeErasedBox requestN() {
  return erase_and_box(ControlMessage{.payload = RequestN{.n = 1}});
}

} // namespace

// =============================================================================
// The tail is the inbound backstop: a producer consumes all demand, so anything
// exiting the pipeline here is unhandled — a violation, fatal in debug and
// Result::Error in release.
// =============================================================================

TEST(ProducerTailAdapterDeathTest, UnhandledDemandIsRejected) {
  Handler handler;
  StubContext ctx;

  if (folly::kIsDebug) {
    EXPECT_DEATH((void)handler.onRead(ctx, requestN()), "unhandled");
  } else {
    EXPECT_EQ(handler.onRead(ctx, requestN()), Result::Error);
  }
}

// =============================================================================
// Exceptions reaching the tail terminate there; there is nothing downstream of
// a producing-end tail to forward them to.
// =============================================================================

TEST(ProducerTailAdapterTest, ExceptionsTerminateAtTail) {
  Handler handler;

  handler.onException(
      folly::make_exception_wrapper<std::runtime_error>("boom"));
}

} // namespace apache::thrift::fast_thrift::thrift::stream
