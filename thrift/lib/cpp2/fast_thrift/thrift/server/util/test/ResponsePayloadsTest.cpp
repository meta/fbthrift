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

#include <gtest/gtest.h>

#include <memory>
#include <utility>

#include <folly/io/IOBuf.h>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ResponsePayloads.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::fast_thrift::thrift {

namespace {

constexpr uint32_t kStreamId = 7;

folly::EventBase& requestContextEventBase() {
  static folly::EventBase eventBase;
  return eventBase;
}

// An initial-response message carrying a request context, as the completion
// thunks hand it to the adapter's write funnel.
ThriftServerResponseMessage makeResponseWithContext(
    ThriftRequestContextPtr requestContext) {
  auto message = makeResponseMessage(
      kStreamId,
      folly::IOBuf::copyBuffer("response"),
      std::make_unique<apache::thrift::ResponseRpcMetadata>());
  message.requestContext = std::move(requestContext);
  return message;
}

const apache::thrift::ResponseRpcMetadata& metadataOf(
    const ThriftServerResponseMessage& message) {
  return *message.payload.get<ThriftInitialResponsePayload>().metadata;
}

} // namespace

TEST(ResponsePayloadsTest, HandlerSetHeadersLandOnResponseMetadata) {
  auto requestContext = makeThriftRequestContext(requestContextEventBase());
  requestContext->setResponseHeader("shard", "42");
  requestContext->setResponseHeader("tier", "primary");
  auto message = makeResponseWithContext(std::move(requestContext));

  attachResponseHeaders(message);

  const ThriftRequestContext::HeaderMap expected{
      {"shard", "42"}, {"tier", "primary"}};
  ASSERT_TRUE(metadataOf(message).otherMetadata().has_value());
  EXPECT_EQ(*metadataOf(message).otherMetadata(), expected);
}

TEST(ResponsePayloadsTest, HandlerSetHeadersLandOnStreamInitialMetadata) {
  auto requestContext = makeThriftRequestContext(requestContextEventBase());
  requestContext->setResponseHeader("shard", "42");
  ThriftServerResponseMessage message{
      .requestContext = std::move(requestContext),
      .payload = ThriftStreamInitialResponsePayload{
          .data = folly::IOBuf::copyBuffer("initial response"),
          .metadata = std::make_unique<apache::thrift::ResponseRpcMetadata>(),
          .streamId = kStreamId,
          .complete = false,
          .next = true,
      }};

  attachResponseHeaders(message);

  const auto& metadata =
      *message.payload.get<ThriftStreamInitialResponsePayload>().metadata;
  const ThriftRequestContext::HeaderMap expected{{"shard", "42"}};
  ASSERT_TRUE(metadata.otherMetadata().has_value());
  EXPECT_EQ(*metadata.otherMetadata(), expected);
}

// The common case must not materialize the optional field at all: an empty
// otherMetadata would still cost a map entry on the wire.
TEST(ResponsePayloadsTest, NoHandlerHeadersLeavesOtherMetadataUnset) {
  auto message = makeResponseWithContext(
      makeThriftRequestContext(requestContextEventBase()));

  attachResponseHeaders(message);

  EXPECT_FALSE(metadataOf(message).otherMetadata().has_value());
}

// The context's headers join what a metadata producer already put there rather
// than replacing the field wholesale, and win on a shared key.
TEST(ResponsePayloadsTest, HandlerSetHeadersMergeWithExistingMetadata) {
  auto requestContext = makeThriftRequestContext(requestContextEventBase());
  requestContext->setResponseHeader("shard", "42");
  requestContext->setResponseHeader("tier", "primary");
  auto message = makeResponseWithContext(std::move(requestContext));
  message.payload.get<ThriftInitialResponsePayload>()
      .metadata->otherMetadata()
      .ensure() = {{"tier", "stale"}, {"origin", "framework"}};

  attachResponseHeaders(message);

  const ThriftRequestContext::HeaderMap expected{
      {"shard", "42"}, {"tier", "primary"}, {"origin", "framework"}};
  ASSERT_TRUE(metadataOf(message).otherMetadata().has_value());
  EXPECT_EQ(*metadataOf(message).otherMetadata(), expected);
}

// Headers are handed over, not copied — a second response built from the same
// context (there is none in practice, but the contract should be explicit)
// must not resend them.
TEST(ResponsePayloadsTest, HeadersAreExtractedFromTheContext) {
  auto requestContext = makeThriftRequestContext(requestContextEventBase());
  requestContext->setResponseHeader("shard", "42");
  auto* requestContextPtr = requestContext.get();
  auto message = makeResponseWithContext(std::move(requestContext));

  attachResponseHeaders(message);

  EXPECT_FALSE(requestContextPtr->hasResponseHeaders());
}

// Responses created before request-context construction carry no context.
TEST(ResponsePayloadsTest, MissingRequestContextIsIgnored) {
  auto message = makeResponseMessage(
      kStreamId,
      folly::IOBuf::copyBuffer("response"),
      std::make_unique<apache::thrift::ResponseRpcMetadata>());

  attachResponseHeaders(message);

  EXPECT_FALSE(metadataOf(message).otherMetadata().has_value());
}

// Rocket ERROR frames have no ResponseRpcMetadata to carry headers on.
TEST(ResponsePayloadsTest, ErrorPayloadIsSkipped) {
  auto requestContext = makeThriftRequestContext(requestContextEventBase());
  requestContext->setResponseHeader("shard", "42");
  auto message = makeFrameworkErrorMessage(
      kStreamId,
      apache::thrift::ResponseRpcErrorCode::UNKNOWN_METHOD,
      "no such method");
  message.requestContext = std::move(requestContext);

  attachResponseHeaders(message);

  EXPECT_TRUE(message.payload.is<ThriftErrorPayload>());
}

} // namespace apache::thrift::fast_thrift::thrift
