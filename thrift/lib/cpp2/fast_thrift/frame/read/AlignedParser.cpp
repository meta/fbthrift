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

#include <thrift/lib/cpp2/fast_thrift/frame/read/AlignedParser.h>

#include <algorithm>
#include <cstdint>
#include <cstring>

#include <folly/io/Cursor.h>

#include <thrift/lib/cpp2/fast_thrift/frame/FrameDescriptor.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameParser.h>

namespace apache::thrift::fast_thrift::frame::read {

AlignedParser::AlignedParser(size_t maxFrameSize) noexcept
    : maxFrameSize_(maxFrameSize), remainingHeader_(kHeaderSize) {}

channel_pipeline::BytesPtr AlignedParser::newBuffer(size_t capacity) noexcept {
  channel_pipeline::BytesPtr buffer =
      bufFactory_ ? (*bufFactory_)(capacity) : folly::IOBuf::create(capacity);
  if (!buffer || buffer->isChained() || buffer->isSharedOne() ||
      !buffer->empty() || buffer->tailroom() < capacity) [[unlikely]] {
    return nullptr;
  }
  return buffer;
}

bool AlignedParser::startHeaderAndMetadata() noexcept {
  const size_t frameHeaderSize = headerBytesRead_ - kMetadataLengthSize;
  headerAndMetadata_ = newBuffer(frameHeaderSize + remainingMetadata_);
  if (!headerAndMetadata_) {
    return false;
  }
  std::memcpy(
      headerAndMetadata_->writableTail(),
      headerBytes_.data() + kMetadataLengthSize,
      frameHeaderSize);
  headerAndMetadata_->append(frameHeaderSize);
  return true;
}

bool AlignedParser::startBody() noexcept {
  channel_pipeline::BytesPtr body = newBuffer(frameLength_);
  if (!body) {
    return false;
  }
  std::memcpy(
      body->writableTail(),
      headerBytes_.data() + kMetadataLengthSize,
      kBaseHeaderSize);
  body->append(kBaseHeaderSize);
  body_.append(std::move(body));
  return true;
}

bool AlignedParser::hasOwnBuffers() const noexcept {
  // startOwnBuffers and onMetadataLengthBytes both measure from
  // kBaseHeaderSize. A frame type that carries extra header bytes would push
  // both by that many, so it stays on the plain path until someone handles it.
  return (frameType_ == FrameType::REQUEST_RESPONSE ||
          frameType_ == FrameType::PAYLOAD) &&
      getDescriptor(frameType_).headerSize == kBaseHeaderSize;
}

bool AlignedParser::needsAlignment() const noexcept {
  return frameType_ == FrameType::REQUEST_RESPONSE;
}

channel_pipeline::BytesPtr AlignedParser::newDataBuffer() noexcept {
  // Without the shift there is nothing to leave room for, so the buffer is
  // exactly the size of the data.
  if (!needsAlignment()) {
    return newBuffer(remainingData_);
  }

  // Room for the data plus the shift below, which is under kAlignment bytes.
  channel_pipeline::BytesPtr buf = newBuffer(remainingData_ + kAlignment);
  if (!buf) {
    return nullptr;
  }

  // The blob sits kBytesBeforeFirstField into the data field, so move the
  // start until the blob lands on a kAlignment boundary. A buffer that already
  // starts on a boundary gives a shift of 6, which is the classic case. We
  // measure it instead of assuming it, so the parser works with any factory.
  const uintptr_t start = reinterpret_cast<uintptr_t>(buf->writableTail());
  const size_t shift =
      (kAlignment - ((start + kBytesBeforeFirstField) % kAlignment)) %
      kAlignment;
  buf->advance(shift);
  return buf;
}

void AlignedParser::getReadBuffer(
    void** bufReturn, size_t* lenReturn) noexcept {
  switch (state_) {
    case State::AwaitingHeader:
    case State::AwaitingMetadataLength:
      headerReadBuffer(bufReturn, lenReturn);
      return;
    case State::AwaitingMetadata:
      metadataReadBuffer(bufReturn, lenReturn);
      return;
    case State::AwaitingData:
      dataReadBuffer(bufReturn, lenReturn);
      return;
    case State::AwaitingBody:
      bodyReadBuffer(bufReturn, lenReturn);
      return;
    case State::Error:
      *bufReturn = nullptr;
      *lenReturn = 0;
      return;
  }
}

void AlignedParser::headerReadBuffer(
    void** bufReturn, size_t* lenReturn) noexcept {
  *bufReturn = headerBytes_.data() + headerBytesRead_;
  *lenReturn = remainingHeader_;
}

void AlignedParser::metadataReadBuffer(
    void** bufReturn, size_t* lenReturn) noexcept {
  *bufReturn = headerAndMetadata_->writableTail();
  *lenReturn = std::min(headerAndMetadata_->tailroom(), remainingMetadata_);
}

void AlignedParser::dataReadBuffer(
    void** bufReturn, size_t* lenReturn) noexcept {
  if (!data_) {
    data_ = newDataBuffer();
    if (!data_) {
      state_ = State::Error;
      *bufReturn = nullptr;
      *lenReturn = 0;
      return;
    }
  }
  *bufReturn = data_->writableTail();
  *lenReturn = std::min(data_->tailroom(), remainingData_);
}

void AlignedParser::bodyReadBuffer(
    void** bufReturn, size_t* lenReturn) noexcept {
  if (body_.empty() && !startBody()) {
    state_ = State::Error;
    *bufReturn = nullptr;
    *lenReturn = 0;
    return;
  }
  *bufReturn = body_.writableTail();
  *lenReturn = std::min(body_.tailroom(), remainingBody_);
}

AlignedParser::Step AlignedParser::onBytes(size_t len) noexcept {
  switch (state_) {
    case State::AwaitingHeader:
      return onHeaderBytes(len);
    case State::AwaitingMetadataLength:
      return onMetadataLengthBytes(len);
    case State::AwaitingMetadata:
      return onMetadataBytes(len);
    case State::AwaitingData:
      return onDataBytes(len);
    case State::AwaitingBody:
      return onBodyBytes(len);
    case State::Error:
      return Step::Bad;
  }
  return Step::Bad;
}

AlignedParser::Step AlignedParser::onHeaderBytes(size_t len) noexcept {
  headerBytesRead_ += len;
  remainingHeader_ -= len;
  if (remainingHeader_ > 0) {
    return Step::NeedMore;
  }

  folly::IOBuf header =
      folly::IOBuf::wrapBufferAsValue(headerBytes_.data(), headerBytesRead_);
  folly::io::Cursor cursor{&header};
  frameLength_ = detail::readFrameOrMetadataSize(cursor);

  // The length a frame gives must at least cover the header itself.
  if (frameLength_ < kBaseHeaderSize) [[unlikely]] {
    return Step::Bad;
  }
  // Refuse before anything is allocated. If we checked later, we would first
  // reserve the exact size we then turn down.
  if (frameLength_ > maxFrameSize_) [[unlikely]] {
    return Step::Bad;
  }

  cursor.skip(kStreamIdSize);
  const auto [frameTypeRaw, flags] = detail::readFrameTypeAndFlags(cursor);
  frameType_ = static_cast<FrameType>(frameTypeRaw);
  if (hasOwnBuffers()) {
    return startOwnBuffers(flags);
  }

  remainingBody_ = frameLength_ - kBaseHeaderSize;
  if (remainingBody_ == 0) {
    // Nothing after the header. A CANCEL frame looks like this.
    return startBody() ? Step::FrameReady : Step::Bad;
  }

  state_ = State::AwaitingBody;
  return Step::NeedMore;
}

AlignedParser::Step AlignedParser::startOwnBuffers(uint16_t flags) noexcept {
  if (flags & frame::detail::kMetadataBit) {
    // Three more header bytes hold the metadata length.
    if (frameLength_ < kBaseHeaderSize + kMetadataLengthSize) [[unlikely]] {
      return Step::Bad;
    }
    remainingHeader_ = kMetadataLengthSize;
    state_ = State::AwaitingMetadataLength;
    return Step::NeedMore;
  }

  if (!startHeaderAndMetadata()) {
    return Step::Bad;
  }
  remainingData_ = frameLength_ - kBaseHeaderSize;
  if (remainingData_ == 0) {
    return Step::FrameReady;
  }
  state_ = State::AwaitingData;
  return Step::NeedMore;
}

AlignedParser::Step AlignedParser::onMetadataLengthBytes(size_t len) noexcept {
  headerBytesRead_ += len;
  remainingHeader_ -= len;
  if (remainingHeader_ > 0) {
    return Step::NeedMore;
  }

  folly::IOBuf header =
      folly::IOBuf::wrapBufferAsValue(headerBytes_.data(), headerBytesRead_);
  folly::io::Cursor cursor{&header};
  cursor.skip(kHeaderSize);
  remainingMetadata_ = detail::readFrameOrMetadataSize(cursor);

  const size_t budget = frameLength_ - kBaseHeaderSize - kMetadataLengthSize;
  if (remainingMetadata_ > budget) [[unlikely]] {
    return Step::Bad;
  }
  remainingData_ = budget - remainingMetadata_;
  if (!startHeaderAndMetadata()) {
    return Step::Bad;
  }

  if (remainingMetadata_ > 0) {
    state_ = State::AwaitingMetadata;
    return Step::NeedMore;
  }
  if (remainingData_ == 0) {
    return Step::FrameReady;
  }
  state_ = State::AwaitingData;
  return Step::NeedMore;
}

AlignedParser::Step AlignedParser::onMetadataBytes(size_t len) noexcept {
  headerAndMetadata_->append(len);
  remainingMetadata_ -= len;
  if (remainingMetadata_ > 0) {
    return Step::NeedMore;
  }
  if (remainingData_ == 0) {
    return Step::FrameReady;
  }
  state_ = State::AwaitingData;
  return Step::NeedMore;
}

AlignedParser::Step AlignedParser::onDataBytes(size_t len) noexcept {
  data_->append(len);
  remainingData_ -= len;
  return remainingData_ == 0 ? Step::FrameReady : Step::NeedMore;
}

AlignedParser::Step AlignedParser::onBodyBytes(size_t len) noexcept {
  body_.postallocate(len);
  remainingBody_ -= len;
  return remainingBody_ == 0 ? Step::FrameReady : Step::NeedMore;
}

channel_pipeline::BytesPtr AlignedParser::takeFrame() noexcept {
  channel_pipeline::BytesPtr frame;
  if (headerAndMetadata_) {
    frame = std::move(headerAndMetadata_);
    if (data_) {
      frame->appendToChain(std::move(data_));
    }
  } else {
    frame = body_.split(frameLength_);
  }
  // Clear the framing state before the sink runs, because it may re-enter.
  startNextFrame();
  return frame;
}

void AlignedParser::startNextFrame() noexcept {
  state_ = State::AwaitingHeader;
  frameType_ = FrameType::RESERVED;
  remainingHeader_ = kHeaderSize;
  headerBytesRead_ = 0;
  remainingMetadata_ = 0;
  remainingData_ = 0;
  remainingBody_ = 0;
  frameLength_ = 0;
}

void AlignedParser::setIOBufFactory(folly::IOBufFactory* factory) noexcept {
  bufFactory_ = factory;
}

void AlignedParser::reset() noexcept {
  headerAndMetadata_.reset();
  data_.reset();
  body_.reset();
  startNextFrame();
}

} // namespace apache::thrift::fast_thrift::frame::read
