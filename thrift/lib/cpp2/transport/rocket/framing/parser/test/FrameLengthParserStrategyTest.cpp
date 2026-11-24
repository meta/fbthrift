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

#include <type_traits>

#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp2/IOBufChainCursor.h>
#include <thrift/lib/cpp2/transport/rocket/framing/parser/FrameLengthParserStrategy.h>
#include <thrift/lib/cpp2/transport/rocket/framing/parser/test/TestUtil.h>

namespace apache::thrift::rocket {
namespace {

std::unique_ptr<folly::IOBuf> makeBorrowedBuffer(
    void* data, size_t size, size_t& releaseCount) {
  return folly::IOBuf::takeOwnership(
      data,
      size,
      [](void*, void* userData) { ++*static_cast<size_t*>(userData); },
      &releaseCount);
}

std::string serializeFrame(const std::string& payload) {
  std::string frame(Serializer::kBytesForFrameOrMetadataLength, '\0');
  HeaderSerializer serializer(
      reinterpret_cast<uint8_t*>(frame.data()), frame.size());
  serializer.writeFrameOrMetadataSize(payload.size());
  frame.append(payload);
  return frame;
}

} // namespace

template <typename Buffer>
class FrameLengthParserTest : public ::testing::Test {
 protected:
  using Parser = FrameLengthParserStrategy<FakeOwner, Buffer>;

  class ReadInput {
   public:
    explicit ReadInput(Parser& parser) : parser_(parser) {}

    void prepareRead(void** data, size_t* size) {
      if constexpr (std::is_same_v<Buffer, IOBufChain>) {
        pending_ = folly::IOBuf::create(4096);
        pending_->append(pending_->capacity());
        *data = pending_->writableData();
        *size = pending_->length();
      } else {
        parser_.getReadBuffer(data, size);
      }
    }

    void completeRead(size_t size) {
      if constexpr (std::is_same_v<Buffer, IOBufChain>) {
        auto bytes = pending_->clone();
        bytes->trimEnd(bytes->length() - size);
        pending_->trimStart(size);
        parser_.readBufferAvailable(std::move(bytes));
      } else {
        parser_.readDataAvailable(size);
      }
    }

   private:
    Parser& parser_;
    std::unique_ptr<folly::IOBuf> pending_;
  };

  static auto& frames(FakeOwner& owner) {
    if constexpr (std::is_same_v<Buffer, IOBufChain>) {
      return owner.chainFrames_;
    } else {
      return owner.frames_;
    }
  }

  static size_t frameSize(const Buffer& frame) {
    if constexpr (std::is_same_v<Buffer, IOBufChain>) {
      return frame.chainLength();
    } else {
      return frame->computeChainDataLength();
    }
  }

  static std::string frameData(const Buffer& frame) {
    if constexpr (std::is_same_v<Buffer, IOBufChain>) {
      return io::IOBufChainCursor(frame).readFixedString(frame.chainLength());
    } else {
      return frame->template to<std::string>();
    }
  }
};

using ParserBuffers =
    ::testing::Types<std::unique_ptr<folly::IOBuf>, IOBufChain>;
TYPED_TEST_SUITE(FrameLengthParserTest, ParserBuffers);

TYPED_TEST(FrameLengthParserTest, testAppendFrame) {
  FakeOwner owner;
  typename TestFixture::Parser parser(owner);
  typename TestFixture::ReadInput input(parser);

  void* buf;
  size_t lenReturn;
  input.prepareRead(&buf, &lenReturn);

  HeaderSerializer serializer(static_cast<uint8_t*>(buf), lenReturn);
  serializer.writeFrameOrMetadataSize(20);
  std::string b(20, 'b');
  memcpy(&static_cast<uint8_t*>(buf)[3], b.data(), 20);

  input.completeRead(23);

  EXPECT_EQ(parser.getSize(), 0);
  EXPECT_EQ(parser.getFrameLength(), 0);
  EXPECT_EQ(parser.getFrameLengthAndFieldSize(), 0);
  EXPECT_EQ(owner.memoryCounter_, 0);
  ASSERT_EQ(this->frames(owner).size(), 1);

  auto frame = std::move(this->frames(owner)[0]);
  EXPECT_EQ(this->frameSize(frame), 20);
  EXPECT_EQ(this->frameData(frame), b);
}

TYPED_TEST(FrameLengthParserTest, testAppendLessThanFullFrame) {
  FakeOwner owner;
  typename TestFixture::Parser parser(owner);
  typename TestFixture::ReadInput input(parser);

  void* buf;
  size_t lenReturn;
  input.prepareRead(&buf, &lenReturn);

  HeaderSerializer serializer(static_cast<uint8_t*>(buf), lenReturn);
  serializer.writeFrameOrMetadataSize(20);
  std::string b(20, 'b');
  memcpy(&static_cast<uint8_t*>(buf)[3], b.data(), 20);

  input.completeRead(10);

  EXPECT_EQ(parser.getSize(), 10);
  EXPECT_EQ(parser.getFrameLength(), 20);
  EXPECT_EQ(parser.getFrameLengthAndFieldSize(), 23);
  EXPECT_EQ(owner.memoryCounter_, 23);
  EXPECT_EQ(this->frames(owner).size(), 0);
}

TYPED_TEST(FrameLengthParserTest, testAppendLessTwiceAndGetAFrame) {
  FakeOwner owner;
  typename TestFixture::Parser parser(owner);
  typename TestFixture::ReadInput input(parser);

  void* buf;
  size_t lenReturn;
  input.prepareRead(&buf, &lenReturn);

  HeaderSerializer serializer(static_cast<uint8_t*>(buf), lenReturn);
  serializer.writeFrameOrMetadataSize(20);
  std::string b(20, 'b');
  memcpy(&static_cast<uint8_t*>(buf)[3], b.data(), 20);

  input.completeRead(10);

  EXPECT_EQ(parser.getSize(), 10);
  EXPECT_EQ(parser.getFrameLength(), 20);
  EXPECT_EQ(parser.getFrameLengthAndFieldSize(), 23);
  EXPECT_EQ(owner.memoryCounter_, 23);
  EXPECT_EQ(this->frames(owner).size(), 0);

  input.completeRead(13);

  EXPECT_EQ(parser.getSize(), 0);
  EXPECT_EQ(parser.getFrameLength(), 0);
  EXPECT_EQ(parser.getFrameLengthAndFieldSize(), 0);
  EXPECT_EQ(owner.memoryCounter_, 0);
  ASSERT_EQ(this->frames(owner).size(), 1);
  EXPECT_EQ(this->frameData(this->frames(owner)[0]), std::string(20, 'b'));
}

TYPED_TEST(FrameLengthParserTest, testAppendMultipleFrames) {
  FakeOwner owner;
  typename TestFixture::Parser parser(owner);
  typename TestFixture::ReadInput input(parser);

  {
    void* buf;
    size_t lenReturn;
    input.prepareRead(&buf, &lenReturn);

    HeaderSerializer serializer(static_cast<uint8_t*>(buf), lenReturn);
    serializer.writeFrameOrMetadataSize(20);
    std::string b(20, 'b');
    memcpy(&static_cast<uint8_t*>(buf)[3], b.data(), 20);

    input.completeRead(23);

    EXPECT_EQ(parser.getSize(), 0);
    EXPECT_EQ(parser.getFrameLength(), 0);
    EXPECT_EQ(parser.getFrameLengthAndFieldSize(), 0);
    EXPECT_EQ(owner.memoryCounter_, 0);
    EXPECT_EQ(this->frames(owner).size(), 1);
  }

  {
    void* buf;
    size_t lenReturn;
    input.prepareRead(&buf, &lenReturn);

    HeaderSerializer serializer(static_cast<uint8_t*>(buf), lenReturn);
    serializer.writeFrameOrMetadataSize(20);
    std::string b(20, 'b');
    memcpy(&static_cast<uint8_t*>(buf)[3], b.data(), 20);

    input.completeRead(23);

    EXPECT_EQ(parser.getSize(), 0);
    EXPECT_EQ(parser.getFrameLength(), 0);
    EXPECT_EQ(parser.getFrameLengthAndFieldSize(), 0);
    EXPECT_EQ(owner.memoryCounter_, 0);
    EXPECT_EQ(this->frames(owner).size(), 2);
  }

  {
    void* buf;
    size_t lenReturn;
    input.prepareRead(&buf, &lenReturn);

    HeaderSerializer serializer(static_cast<uint8_t*>(buf), lenReturn);
    serializer.writeFrameOrMetadataSize(20);
    std::string b(20, 'b');
    memcpy(&static_cast<uint8_t*>(buf)[3], b.data(), 20);

    input.completeRead(23);

    EXPECT_EQ(parser.getSize(), 0);
    EXPECT_EQ(parser.getFrameLength(), 0);
    EXPECT_EQ(parser.getFrameLengthAndFieldSize(), 0);
    EXPECT_EQ(owner.memoryCounter_, 0);
    EXPECT_EQ(this->frames(owner).size(), 3);
  }
}

TYPED_TEST(FrameLengthParserTest, testAppendUsingIOBuf) {
  FakeOwner owner;
  typename TestFixture::Parser parser(owner);

  folly::IOBufQueue queue =
      folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()};

  auto lenBuf = folly::IOBuf::create(3);
  HeaderSerializer serializer(lenBuf->writableBuffer(), 3);
  serializer.writeFrameOrMetadataSize(20);
  lenBuf->append(3);

  queue.append(std::move(lenBuf));

  EXPECT_EQ(queue.chainLength(), 3);

  auto strBuf = folly::IOBuf::copyBuffer(std::string(20, 'b'));

  EXPECT_EQ(strBuf->length(), 20);

  queue.append(std::move(strBuf));

  EXPECT_EQ(queue.chainLength(), 23);

  auto buf = queue.split(23);

  EXPECT_EQ(buf->computeChainDataLength(), 23);
  EXPECT_EQ(queue.chainLength(), 0);

  parser.readBufferAvailable(std::move(buf));

  EXPECT_EQ(parser.getSize(), 0);
  EXPECT_EQ(parser.getFrameLength(), 0);
  EXPECT_EQ(parser.getFrameLengthAndFieldSize(), 0);
  EXPECT_EQ(owner.memoryCounter_, 0);
  ASSERT_EQ(this->frames(owner).size(), 1);
  EXPECT_EQ(this->frameData(this->frames(owner)[0]), std::string(20, 'b'));
}

TEST(FrameLengthParserTest, testCopiesFrameIfAnyBufferIsScarce) {
  const std::string payload = "frame split across receive buffers";
  auto frame = serializeFrame(payload);
  const auto firstBufferSize = Serializer::kBytesForFrameOrMetadataLength + 8;
  const auto secondBufferSize = 8;
  size_t releaseCount = 0;
  FakeOwner owner;
  FrameLengthParserStrategy<FakeOwner> parser(owner);

  parser.setBuffersScarce(false);
  parser.readBufferAvailable(
      makeBorrowedBuffer(frame.data(), firstBufferSize, releaseCount));
  parser.setBuffersScarce(true);
  parser.readBufferAvailable(makeBorrowedBuffer(
      frame.data() + firstBufferSize, secondBufferSize, releaseCount));
  EXPECT_TRUE(owner.frames_.empty());

  parser.setBuffersScarce(false);
  parser.readBufferAvailable(makeBorrowedBuffer(
      frame.data() + firstBufferSize + secondBufferSize,
      frame.size() - firstBufferSize - secondBufferSize,
      releaseCount));

  ASSERT_EQ(owner.frames_.size(), 1);
  EXPECT_EQ(releaseCount, 3);
  EXPECT_EQ(owner.frames_[0]->to<std::string>(), payload);
}

TEST(FrameLengthParserTest, testCopiesEveryFrameInScarceBufferAndResets) {
  const std::string firstPayload = "first frame";
  const std::string secondPayload = "second frame";
  const std::string thirdPayload = "third frame";
  auto scarceData =
      serializeFrame(firstPayload) + serializeFrame(secondPayload);
  auto unscarceData = serializeFrame(thirdPayload);
  size_t releaseCount = 0;
  FakeOwner owner;
  FrameLengthParserStrategy<FakeOwner> parser(owner);
  parser.setBuffersScarce(true);

  parser.readBufferAvailable(
      makeBorrowedBuffer(scarceData.data(), scarceData.size(), releaseCount));

  ASSERT_EQ(owner.frames_.size(), 2);
  EXPECT_EQ(releaseCount, 1);
  EXPECT_EQ(owner.frames_[0]->to<std::string>(), firstPayload);
  EXPECT_EQ(owner.frames_[1]->to<std::string>(), secondPayload);

  parser.setBuffersScarce(false);
  parser.readBufferAvailable(makeBorrowedBuffer(
      unscarceData.data(), unscarceData.size(), releaseCount));

  ASSERT_EQ(owner.frames_.size(), 3);
  EXPECT_EQ(releaseCount, 1);
  EXPECT_EQ(owner.frames_[2]->to<std::string>(), thirdPayload);

  owner.frames_.clear();
  EXPECT_EQ(releaseCount, 2);
}

} // namespace apache::thrift::rocket
