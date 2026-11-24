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

// What is particular to FrameLengthParser. The framing behaviour it shares
// with every other parser is covered by ParserContractTest.cpp.

#include <cstring>
#include <vector>

#include <gtest/gtest.h>

#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/portability/GMock.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameLengthParser.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameLength.h>

namespace apache::thrift::fast_thrift::frame::read {

using namespace testing;

using apache::thrift::fast_thrift::channel_pipeline::BytesPtr;
using apache::thrift::fast_thrift::channel_pipeline::Result;

class FrameLengthParserTest : public Test {
 protected:
  static BytesPtr buildFrame(size_t payloadSize) {
    auto buf = folly::IOBuf::create(kMetadataLengthSize + payloadSize);
    write::writeFrameLength(buf->writableData(), payloadSize);
    std::memset(buf->writableData() + kMetadataLengthSize, 'x', payloadSize);
    buf->append(kMetadataLengthSize + payloadSize);
    return buf;
  }

  static BytesPtr buildHeader(size_t payloadSize) {
    auto buf = folly::IOBuf::create(kMetadataLengthSize);
    write::writeFrameLength(buf->writableData(), payloadSize);
    buf->append(kMetadataLengthSize);
    return buf;
  }

  static BytesPtr buildPayload(size_t size) {
    auto buf = folly::IOBuf::create(size);
    std::memset(buf->writableData(), 'x', size);
    buf->append(size);
    return buf;
  }

  // Collects emitted frames and replays whatever Result the test asked for.
  auto sink() noexcept {
    return [this](BytesPtr&& frame) noexcept {
      frames_.push_back(std::move(frame));
      return sinkResult_;
    };
  }

  Result feed(BytesPtr buf) {
    return parser_.consumeBuffer(std::move(buf), sink());
  }

  // getReadBuffer + consume, where feed() above goes through consumeBuffer.
  Result feedViaReadBuffer(const BytesPtr& bytes) {
    const auto len = bytes->computeChainDataLength();
    void* buf = nullptr;
    size_t avail = 0;
    parser_.getReadBuffer(&buf, &avail);
    if (avail < len) {
      ADD_FAILURE() << "parser offered " << avail << " bytes of room for "
                    << len << " bytes";
      return Result::Error;
    }
    size_t offset = 0;
    for (const auto& range : *bytes) {
      std::memcpy(
          static_cast<uint8_t*>(buf) + offset, range.data(), range.size());
      offset += range.size();
    }
    return parser_.consume(len, sink());
  }

  std::vector<BytesPtr> frames_;
  Result sinkResult_{Result::Success};
  FrameLengthParser parser_;
};

// The framing accessors are this parser's own surface; the shared contract
// suite cannot see them.
TEST_F(FrameLengthParserTest, TracksFramingStateAcrossAFrame) {
  auto partial = folly::IOBuf::create(2);
  partial->writableData()[0] = 0x00;
  partial->writableData()[1] = 0x00;
  partial->append(2);

  EXPECT_EQ(feed(std::move(partial)), Result::Success);
  EXPECT_EQ(parser_.size(), 2);
  EXPECT_EQ(parser_.frameLength(), 0);

  parser_.reset();

  EXPECT_EQ(feed(buildHeader(20)), Result::Success);
  EXPECT_EQ(parser_.size(), 3);
  EXPECT_EQ(parser_.frameLength(), 20);
  EXPECT_EQ(parser_.frameLengthAndFieldSize(), 23);

  EXPECT_EQ(feed(buildPayload(20)), Result::Success);
  EXPECT_EQ(parser_.size(), 0);
  EXPECT_EQ(parser_.frameLength(), 0);
  EXPECT_EQ(parser_.frameLengthAndFieldSize(), 0);
  ASSERT_EQ(frames_.size(), 1);
  EXPECT_EQ(frames_[0]->computeChainDataLength(), 20);
}

TEST_F(FrameLengthParserTest, ChainedIOBuf) {
  auto header = buildHeader(20);
  header->appendToChain(buildPayload(20));
  EXPECT_EQ(header->computeChainDataLength(), 23);

  EXPECT_EQ(feed(std::move(header)), Result::Success);
  EXPECT_EQ(parser_.size(), 0);
  ASSERT_EQ(frames_.size(), 1);
  EXPECT_EQ(frames_[0]->computeChainDataLength(), 20);
}

TEST_F(FrameLengthParserTest, EmptyFrame) {
  EXPECT_EQ(feed(buildFrame(0)), Result::Success);
  ASSERT_EQ(frames_.size(), 1);
  EXPECT_EQ(frames_[0]->computeChainDataLength(), 0);
}

TEST_F(FrameLengthParserTest, BackpressureThenResume) {
  sinkResult_ = Result::Backpressure;

  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  queue.append(buildFrame(20));
  queue.append(buildFrame(30));

  EXPECT_EQ(feed(queue.move()), Result::Backpressure);
  ASSERT_EQ(frames_.size(), 1);
  EXPECT_EQ(frames_[0]->computeChainDataLength(), 20);
  EXPECT_GT(parser_.size(), 0);

  sinkResult_ = Result::Success;
  EXPECT_EQ(parser_.resume(sink()), Result::Success);

  ASSERT_EQ(frames_.size(), 2);
  EXPECT_EQ(frames_[1]->computeChainDataLength(), 30);
  EXPECT_EQ(parser_.size(), 0);
}

// Same as BackpressureThenResume, but through consume. Bytes the parser has
// already taken must survive the refusal and come out when the sink recovers.
TEST_F(FrameLengthParserTest, BackpressureThenResumeOnTheConsumePath) {
  sinkResult_ = Result::Backpressure;

  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  queue.append(buildFrame(20));
  queue.append(buildFrame(30));

  EXPECT_THAT(feedViaReadBuffer(queue.move()), Eq(Result::Backpressure));
  ASSERT_THAT(frames_, SizeIs(1));
  EXPECT_THAT(frames_[0]->computeChainDataLength(), Eq(20));
  EXPECT_THAT(parser_.size(), Gt(0));

  sinkResult_ = Result::Success;
  EXPECT_THAT(parser_.resume(sink()), Eq(Result::Success));

  ASSERT_THAT(frames_, SizeIs(2));
  EXPECT_THAT(frames_[1]->computeChainDataLength(), Eq(30));
  EXPECT_THAT(parser_.size(), Eq(0));
}

TEST_F(FrameLengthParserTest, ResetDropsBufferedState) {
  EXPECT_EQ(feed(buildHeader(20)), Result::Success);
  EXPECT_GT(parser_.size(), 0);

  parser_.reset();

  EXPECT_EQ(parser_.size(), 0);
  EXPECT_EQ(parser_.frameLength(), 0);
  EXPECT_EQ(parser_.frameLengthAndFieldSize(), 0);
}

// --- Buffer management ---

TEST_F(FrameLengthParserTest, ReadBufferIsReusedWhileTailroomRemains) {
  void* first = nullptr;
  size_t len = 0;
  parser_.getReadBuffer(&first, &len);
  ASSERT_NE(first, nullptr);
  EXPECT_GE(len, FrameLengthParser::kDefaultMaxBufferSize);

  // Nothing consumed, so the same tail must come back rather than a new
  // allocation — this reuse is the whole point of the parser owning the buffer.
  void* second = nullptr;
  parser_.getReadBuffer(&second, &len);
  EXPECT_EQ(second, first);
}

TEST_F(FrameLengthParserTest, ReadBufferAdvancesWhileAFrameIsIncomplete) {
  // A frame split across reads is the case reuse matters for: the partial
  // bytes stay put and the next read continues into the same allocation
  // instead of allocating again mid-frame.
  void* first = nullptr;
  size_t len = 0;
  parser_.getReadBuffer(&first, &len);

  EXPECT_EQ(feedViaReadBuffer(buildHeader(20)), Result::Success);
  EXPECT_EQ(frames_.size(), 0);
  EXPECT_EQ(parser_.frameLength(), 20);

  void* second = nullptr;
  parser_.getReadBuffer(&second, &len);
  EXPECT_EQ(second, static_cast<uint8_t*>(first) + kMetadataLengthSize);

  EXPECT_EQ(feedViaReadBuffer(buildPayload(20)), Result::Success);
  ASSERT_EQ(frames_.size(), 1);
  EXPECT_EQ(frames_[0]->computeChainDataLength(), 20);
}

TEST_F(FrameLengthParserTest, DoesNotPreallocateAnAnnouncedLargeFrame) {
  constexpr size_t kFrameSize = 512 * 1024;
  ASSERT_GT(kFrameSize, FrameLengthParser::kDefaultMaxBufferSize);

  EXPECT_EQ(feedViaReadBuffer(buildHeader(kFrameSize)), Result::Success);
  EXPECT_EQ(parser_.frameLength(), kFrameSize);

  // A peer can announce any length up to maxFrameSize, so the announcement
  // alone must not size the buffer — the payload accumulates across reads as
  // it actually arrives. Otherwise a silent peer pins maxFrameSize per
  // connection just by sending a 3-byte prefix.
  void* buf = nullptr;
  size_t len = 0;
  parser_.getReadBuffer(&buf, &len);
  EXPECT_LE(len, FrameLengthParser::kDefaultMaxBufferSize);
}

TEST_F(FrameLengthParserTest, RejectsFrameOverMaxFrameSize) {
  FrameLengthParser parser{
      FrameLengthParser::kDefaultMinBufferSize,
      FrameLengthParser::kDefaultMaxBufferSize,
      /*maxFrameSize=*/1024};

  auto result = parser.consumeBuffer(buildHeader(2048), sink());

  EXPECT_EQ(result, Result::Error);
  EXPECT_EQ(frames_.size(), 0);
}

// An oversized frame must be refused before the parser grows a buffer for it.
// Only consume can reach tryResize; consumeBuffer compiles it out.
TEST_F(FrameLengthParserTest, RejectsFrameOverMaxFrameSizeBeforeGrowing) {
  constexpr size_t kBufferSize = 4096;
  constexpr size_t kMaxFrameSize = 1024 * 1024;

  struct Outcome {
    Result result;
    size_t allocations;
  };

  // Feeds one length prefix to a fresh parser and counts what it allocated
  // after the first read.
  const auto announce = [this](size_t frameLength) {
    size_t allocations = 0;
    folly::IOBufFactory factory = [&allocations](size_t capacity) {
      ++allocations;
      return folly::IOBuf::create(capacity);
    };
    // Declared after the factory it borrows, so it is destroyed first.
    FrameLengthParser parser{kBufferSize, kBufferSize, kMaxFrameSize};
    parser.setIOBufFactory(&factory);

    void* buf = nullptr;
    size_t avail = 0;
    parser.getReadBuffer(&buf, &avail);

    const auto header = buildHeader(frameLength);
    std::memcpy(buf, header->data(), header->length());

    const auto before = allocations;
    const auto result = parser.consume(header->length(), sink());
    return Outcome{result, allocations - before};
  };

  // Control: a frame the parser accepts does make it grow. Without this, a tail
  // long enough to reuse would let the check below pass for the wrong reason.
  const auto accepted = announce(kMaxFrameSize / 2);
  ASSERT_THAT(accepted.result, Eq(Result::Success));
  ASSERT_THAT(accepted.allocations, Gt(0));

  // No allocation means the frame was refused before tryResize ran.
  const auto refused = announce(kMaxFrameSize + 1);
  EXPECT_THAT(refused.result, Eq(Result::Error));
  EXPECT_THAT(refused.allocations, Eq(0));
  EXPECT_THAT(frames_, IsEmpty());
}

TEST_F(FrameLengthParserTest, AllocationsGoThroughTheInstalledFactory) {
  size_t allocations = 0;
  folly::IOBufFactory factory = [&allocations](size_t capacity) {
    ++allocations;
    return folly::IOBuf::create(capacity);
  };
  parser_.setIOBufFactory(&factory);

  void* buf = nullptr;
  size_t len = 0;
  parser_.getReadBuffer(&buf, &len);

  EXPECT_EQ(allocations, 1);
}

} // namespace apache::thrift::fast_thrift::frame::read
