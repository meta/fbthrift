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

#include <thrift/lib/cpp2/transport/rocket/framing/Parser.h>
#include <thrift/lib/cpp2/transport/rocket/framing/parser/FrameLengthParserStrategy.h>

#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp2/transport/rocket/framing/parser/test/TestUtil.h>

namespace apache::thrift::rocket {
namespace {

template <class T>
using IOBufChainFrameLengthParser = FrameLengthParserStrategy<T, IOBufChain>;

struct ReleaseTracker {
  size_t releases{0};
};

struct RejectingOwner {
  void handleFrame(IOBufChain frame) { frames.push_back(std::move(frame)); }
  bool incMemoryUsage(uint32_t) {
    ++admissionAttempts;
    readsStopped = true;
    return false;
  }
  void decMemoryUsage(uint32_t) { ++decrements; }

  std::vector<IOBufChain> frames;
  size_t admissionAttempts{0};
  size_t decrements{0};
  bool readsStopped{false};
};

struct ThrowingOwner {
  // Match the ownership-taking production handler signature.
  // NOLINTNEXTLINE(performance-unnecessary-value-param)
  [[noreturn]] void handleFrame(IOBufChain) {
    ++handledFrames;
    throw std::runtime_error("handler failure");
  }
  bool incMemoryUsage(uint32_t n) {
    memoryCounter += n;
    return true;
  }
  void decMemoryUsage(uint32_t n) { memoryCounter -= n; }

  size_t handledFrames{0};
  uint32_t memoryCounter{0};
};

struct ParserOwner : folly::DelayedDestruction, FakeOwner {
  ~ParserOwner() override = default;

  void close(const transport::TTransportException&) { ++closeCalls; }

  size_t closeCalls{0};
};

void recordRelease(void*, void* userData) noexcept {
  ++static_cast<ReleaseTracker*>(userData)->releases;
}

std::unique_ptr<folly::IOBuf> makeOwnedBuffer(
    void* data, size_t size, ReleaseTracker& tracker) {
  auto buffer = folly::IOBuf::takeOwnership(
      data, size, size, recordRelease, &tracker, true);
  buffer->markExternallySharedOne();
  return buffer;
}

std::vector<uint8_t> makeFrame(std::string_view payload) {
  std::vector<uint8_t> frame(
      Serializer::kBytesForFrameOrMetadataLength + payload.size());
  HeaderSerializer serializer(frame.data(), frame.size());
  serializer.writeFrameOrMetadataSize(payload.size());
  std::memcpy(
      frame.data() + Serializer::kBytesForFrameOrMetadataLength,
      payload.data(),
      payload.size());
  return frame;
}

std::unique_ptr<folly::IOBuf> makeLengthPrefix(size_t frameLength) {
  auto buffer =
      folly::IOBuf::create(Serializer::kBytesForFrameOrMetadataLength);
  HeaderSerializer serializer(buffer->writableData(), buffer->capacity());
  serializer.writeFrameOrMetadataSize(frameLength);
  buffer->append(Serializer::kBytesForFrameOrMetadataLength);
  return buffer;
}

std::string chainString(const IOBufChain& chain) {
  std::string result;
  result.reserve(chain.chainLength());
  for (const auto& buffer : chain) {
    result.append(
        reinterpret_cast<const char*>(buffer.data()), buffer.length());
  }
  return result;
}

TEST(FrameLengthParserIOBufChainTest, CompleteFrameRetainsLease) {
  auto input = makeFrame("payload");
  ReleaseTracker tracker;
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);

  parser.readBufferAvailable(
      makeOwnedBuffer(input.data(), input.size(), tracker));

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_EQ(chainString(owner.chainFrames_.front()), "payload");
  EXPECT_EQ(owner.chainFrames_.front().chainElements(), 1);
  EXPECT_EQ(
      owner.chainFrames_.front().begin()->data(),
      input.data() + Serializer::kBytesForFrameOrMetadataLength);
  EXPECT_EQ(owner.memoryCounter_, 0);
  EXPECT_EQ(tracker.releases, 0);
  owner.chainFrames_.clear();
  EXPECT_EQ(tracker.releases, 1);
}

TEST(FrameLengthParserIOBufChainTest, OuterParserRoutesBufferByMode) {
  auto input = makeFrame("payload");
  ReleaseTracker tracker;
  ParserOwner owner;
  Parser<ParserOwner> parser(owner, "iobuf_chain");

  folly::AsyncTransport::ReadCallback* callback = &parser;
  EXPECT_TRUE(callback->isBufferMovable());
  callback->readBufferAvailable(
      makeOwnedBuffer(input.data(), input.size(), tracker));

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_EQ(chainString(owner.chainFrames_.front()), "payload");
  EXPECT_EQ(owner.closeCalls, 0);
  owner.chainFrames_.clear();
  EXPECT_EQ(tracker.releases, 1);

  ParserOwner defaultOwner;
  Parser<ParserOwner> defaultParser(defaultOwner, "strategy");
  folly::AsyncTransport::ReadCallback* defaultCallback = &defaultParser;
  EXPECT_TRUE(defaultCallback->isBufferMovable());
  ReleaseTracker defaultTracker;
  defaultCallback->readBufferAvailable(
      makeOwnedBuffer(input.data(), input.size(), defaultTracker));

  ASSERT_EQ(defaultOwner.frames_.size(), 1);
  EXPECT_EQ(defaultOwner.frames_.front()->length(), 7);
  EXPECT_TRUE(defaultOwner.chainFrames_.empty());
  EXPECT_EQ(defaultOwner.closeCalls, 0);
  defaultOwner.frames_.clear();
  EXPECT_EQ(defaultTracker.releases, 1);
}

TEST(FrameLengthParserIOBufChainTest, HeaderCanSpanInputBuffers) {
  auto input = makeFrame("abcdef");
  ReleaseTracker tracker;
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);

  parser.readBufferAvailable(folly::IOBuf::copyBuffer(input.data(), 1));
  parser.readBufferAvailable(
      makeOwnedBuffer(input.data() + 1, input.size() - 1, tracker));

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_EQ(chainString(owner.chainFrames_.front()), "abcdef");
  EXPECT_EQ(tracker.releases, 0);
  owner.chainFrames_.clear();
  EXPECT_EQ(tracker.releases, 1);
}

TEST(FrameLengthParserIOBufChainTest, HeaderCanSplitTwoPlusOneAcrossInputs) {
  auto input = makeFrame("abc");
  ReleaseTracker tracker;
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);

  parser.readBufferAvailable(makeOwnedBuffer(input.data(), 2, tracker));
  parser.readBufferAvailable(
      makeOwnedBuffer(input.data() + 2, input.size() - 2, tracker));

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_EQ(chainString(owner.chainFrames_.front()), "abc");
  EXPECT_EQ(tracker.releases, 1);
  owner.chainFrames_.clear();
  EXPECT_EQ(tracker.releases, 2);
}

TEST(FrameLengthParserIOBufChainTest, OneInputCanContainTwoFrames) {
  auto first = makeFrame("abc");
  auto second = makeFrame("defgh");
  std::vector<uint8_t> input;
  input.insert(input.end(), first.begin(), first.end());
  input.insert(input.end(), second.begin(), second.end());
  ReleaseTracker tracker;
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);

  parser.readBufferAvailable(
      makeOwnedBuffer(input.data(), input.size(), tracker));

  ASSERT_EQ(owner.chainFrames_.size(), 2);
  EXPECT_EQ(chainString(owner.chainFrames_[0]), "abc");
  EXPECT_EQ(chainString(owner.chainFrames_[1]), "defgh");
  EXPECT_EQ(owner.chainFrames_[0].chainElements(), 1);
  EXPECT_EQ(owner.chainFrames_[1].chainElements(), 1);
  EXPECT_EQ(tracker.releases, 0);
  owner.chainFrames_.erase(owner.chainFrames_.begin());
  EXPECT_EQ(tracker.releases, 0);
  owner.chainFrames_.clear();
  EXPECT_EQ(tracker.releases, 1);
}

TEST(FrameLengthParserIOBufChainTest, AdjacentInputsRemainSeparateWithinFrame) {
  std::vector<uint8_t> payload(16, 'x');
  ReleaseTracker tracker;
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);
  parser.readBufferAvailable(makeLengthPrefix(payload.size()));

  parser.readBufferAvailable(makeOwnedBuffer(payload.data(), 7, tracker));
  parser.readBufferAvailable(
      makeOwnedBuffer(payload.data() + 7, payload.size() - 7, tracker));

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_EQ(owner.chainFrames_.front().chainElements(), 2);
  EXPECT_EQ(chainString(owner.chainFrames_.front()), std::string(16, 'x'));
  EXPECT_EQ(tracker.releases, 0);
  owner.chainFrames_.clear();
  EXPECT_EQ(tracker.releases, 2);
}

TEST(FrameLengthParserIOBufChainTest, NonAdjacentInputsRemainSeparate) {
  std::vector<uint8_t> allocation(17, 'x');
  ReleaseTracker tracker;
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);
  parser.readBufferAvailable(makeLengthPrefix(16));

  parser.readBufferAvailable(makeOwnedBuffer(allocation.data(), 8, tracker));
  parser.readBufferAvailable(
      makeOwnedBuffer(allocation.data() + 9, 8, tracker));

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_EQ(owner.chainFrames_.front().chainElements(), 2);
  EXPECT_EQ(owner.chainFrames_.front().chainLength(), 16);
  owner.chainFrames_.clear();
  EXPECT_EQ(tracker.releases, 2);
}

TEST(FrameLengthParserIOBufChainTest, AdjacentDataAcrossFramesStaysSeparate) {
  auto first = makeFrame("abcd");
  auto second = makeFrame("efgh");
  std::vector<uint8_t> input;
  input.insert(input.end(), first.begin(), first.end());
  input.insert(input.end(), second.begin(), second.end());
  ReleaseTracker tracker;
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);

  parser.readBufferAvailable(
      makeOwnedBuffer(input.data(), input.size(), tracker));

  ASSERT_EQ(owner.chainFrames_.size(), 2);
  EXPECT_EQ(chainString(owner.chainFrames_[0]), "abcd");
  EXPECT_EQ(chainString(owner.chainFrames_[1]), "efgh");
  EXPECT_EQ(owner.chainFrames_[0].chainElements(), 1);
  EXPECT_EQ(owner.chainFrames_[1].chainElements(), 1);
}

TEST(FrameLengthParserIOBufChainTest, CompleteFrameAndPartialNextAreOrdered) {
  auto first = makeFrame("first");
  auto second = makeFrame("second");
  std::vector<uint8_t> prefix;
  prefix.insert(prefix.end(), first.begin(), first.end());
  prefix.insert(prefix.end(), second.begin(), second.begin() + 2);
  ReleaseTracker tracker;
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);

  parser.readBufferAvailable(
      makeOwnedBuffer(prefix.data(), prefix.size(), tracker));

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_EQ(chainString(owner.chainFrames_.front()), "first");
  EXPECT_EQ(parser.getSize(), 2);

  parser.readBufferAvailable(
      makeOwnedBuffer(second.data() + 2, second.size() - 2, tracker));

  ASSERT_EQ(owner.chainFrames_.size(), 2);
  EXPECT_EQ(chainString(owner.chainFrames_[1]), "second");
  EXPECT_EQ(parser.getSize(), 0);
}

TEST(FrameLengthParserIOBufChainTest, LegacyIOBufInputUsesChainOutput) {
  const auto input = makeFrame("legacy");
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);

  parser.readBufferAvailable(
      folly::IOBuf::copyBuffer(input.data(), input.size()));

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_EQ(chainString(owner.chainFrames_.front()), "legacy");
  EXPECT_TRUE(owner.frames_.empty());
}

TEST(FrameLengthParserIOBufChainTest, MixedOwnershipInputPreservesOrder) {
  std::vector<uint8_t> value{'a', 'b', 'c'};
  ReleaseTracker tracker;
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);
  parser.readBufferAvailable(makeLengthPrefix(6));
  parser.readBufferAvailable(
      makeOwnedBuffer(value.data(), value.size(), tracker));

  parser.readBufferAvailable(folly::IOBuf::copyBuffer("def"));

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_EQ(chainString(owner.chainFrames_.front()), "abcdef");
  EXPECT_EQ(owner.chainFrames_.front().chainElements(), 2);
}

TEST(FrameLengthParserIOBufChainTest, GrowsPastInitialBufferCountEstimate) {
  constexpr size_t kBuffers = 25;
  std::vector<uint8_t> allocation(kBuffers * 2, 'x');
  ReleaseTracker tracker;
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);
  parser.readBufferAvailable(makeLengthPrefix(kBuffers));
  ASSERT_EQ(parser.getFrameBufCountEstimate(), 10);

  for (size_t i = 0; i < kBuffers; ++i) {
    parser.readBufferAvailable(
        makeOwnedBuffer(allocation.data() + i * 2, 1, tracker));
  }

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_EQ(owner.chainFrames_.front().chainElements(), kBuffers);
  EXPECT_GE(owner.chainFrames_.front().capacity(), kBuffers);
  owner.chainFrames_.clear();
  EXPECT_EQ(tracker.releases, kBuffers);
}

TEST(FrameLengthParserIOBufChainTest, RejectedInputReleasesOnDestruction) {
  auto input = makeFrame("data");
  ReleaseTracker tracker;
  RejectingOwner owner;

  {
    IOBufChainFrameLengthParser<RejectingOwner> parser(owner);
    parser.readBufferAvailable(
        makeOwnedBuffer(input.data(), input.size(), tracker));

    EXPECT_EQ(owner.admissionAttempts, 1);
    EXPECT_EQ(owner.decrements, 0);
    EXPECT_TRUE(owner.readsStopped);
    EXPECT_TRUE(owner.frames.empty());
    EXPECT_EQ(tracker.releases, 0);
  }

  EXPECT_EQ(tracker.releases, 1);
}

TEST(FrameLengthParserIOBufChainTest, PartialFrameReleasesOnDestruction) {
  auto input = makeFrame("data");
  ReleaseTracker tracker;
  FakeOwner owner;

  {
    IOBufChainFrameLengthParser<FakeOwner> parser(owner);
    parser.readBufferAvailable(
        makeOwnedBuffer(input.data(), input.size() - 2, tracker));
    EXPECT_EQ(tracker.releases, 0);
  }

  EXPECT_EQ(tracker.releases, 1);
}

TEST(FrameLengthParserIOBufChainTest, MemoryIsReleasedAfterHandlerThrows) {
  auto first = makeFrame("first");
  auto second = makeFrame("second");
  std::vector<uint8_t> input;
  input.insert(input.end(), first.begin(), first.end());
  input.insert(input.end(), second.begin(), second.end());
  ThrowingOwner owner;

  {
    IOBufChainFrameLengthParser<ThrowingOwner> parser(owner);
    EXPECT_THROW(
        parser.readBufferAvailable(
            folly::IOBuf::copyBuffer(input.data(), input.size())),
        std::runtime_error);
    EXPECT_EQ(1, owner.handledFrames);
    EXPECT_EQ(0, owner.memoryCounter);
  }

  EXPECT_EQ(0, owner.memoryCounter);
}

TEST(FrameLengthParserIOBufChainTest, ZeroLengthFrameProducesOneEmptyChain) {
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);

  parser.readBufferAvailable(makeLengthPrefix(0));

  ASSERT_EQ(owner.chainFrames_.size(), 1);
  EXPECT_TRUE(owner.chainFrames_.front().empty());
  EXPECT_EQ(owner.memoryCounter_, 0);
}

TEST(FrameLengthParserIOBufChainTest, ReservesAtLeastTenBuffersAtFrameStart) {
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(owner);
  parser.readBufferAvailable(makeLengthPrefix(4096));

  EXPECT_EQ(parser.getFrameLength(), 4096);
  EXPECT_EQ(parser.getFrameBufCountEstimate(), 10);
  EXPECT_GE(parser.getFrameCapacity(), 10);
  EXPECT_EQ(owner.memoryCounter_, 4099);
}

TEST(FrameLengthParserIOBufChainTest, DecaysBufferLengthEstimate) {
  FakeOwner owner;
  IOBufChainFrameLengthParser<FakeOwner> parser(
      owner, /* minBufferSize */ 16, /* maxBufferSize */ 80);
  parser.readBufferAvailable(makeLengthPrefix(8000));

  EXPECT_EQ(parser.getFrameBufCountEstimate(), 115);

  parser.readBufferAvailable(folly::IOBuf::copyBuffer(std::string(30, 'x')));

  EXPECT_EQ(parser.getFrameBufCountEstimate(), 124);
}

} // namespace
} // namespace apache::thrift::rocket
