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

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/IOBufChain.h>
#include <thrift/lib/cpp2/IOBufChainCursor.h>
#include <thrift/lib/cpp2/transport/rocket/framing/FrameType.h>
#include <thrift/lib/cpp2/transport/rocket/framing/Frames.h>
#include <thrift/lib/cpp2/transport/rocket/framing/Serializer.h>
#include <thrift/lib/cpp2/transport/rocket/framing/Util.h>

namespace apache::thrift::rocket {

namespace detail {

template <typename Buffer>
struct FrameBufferTraits;

template <>
struct FrameBufferTraits<folly::IOBuf> {
  using Storage = folly::IOBufQueue;

  static FOLLY_ALWAYS_INLINE const folly::IOBuf* cursorBuffer(
      const Storage& storage) {
    return storage.front();
  }
};

template <>
struct FrameBufferTraits<IOBufChain> {
  using Storage = IOBufChain;

  static FOLLY_ALWAYS_INLINE const IOBufChain* cursorBuffer(
      const Storage& storage) {
    return &storage;
  }
};

} // namespace detail

template <class T, class Frame = std::unique_ptr<folly::IOBuf>>
class FrameLengthParserStrategy;

template <class T, class Cursor>
class FrameLengthParserStrategyBase {
 public:
  size_t getFrameLength() const noexcept { return frameLength_; }
  size_t getFrameLengthAndFieldSize() const noexcept {
    return frameLengthAndFieldSize_;
  }
  size_t getSize() const noexcept { return size_; }

 protected:
  using Buffer = typename Cursor::Buffer;
  using Traits = detail::FrameBufferTraits<Buffer>;
  using Storage = typename Traits::Storage;

  FrameLengthParserStrategyBase(const FrameLengthParserStrategyBase&) = delete;
  FrameLengthParserStrategyBase& operator=(
      const FrameLengthParserStrategyBase&) = delete;
  FrameLengthParserStrategyBase(FrameLengthParserStrategyBase&&) = delete;
  FrameLengthParserStrategyBase& operator=(FrameLengthParserStrategyBase&&) =
      delete;

  explicit FrameLengthParserStrategyBase(
      T& owner, size_t minBufferSize, size_t maxBufferSize)
      : owner_(owner),
        minBufferSize_(minBufferSize),
        maxBufferSize_(maxBufferSize),
        readBuffer_(makeReadBuffer()),
        cursor_(makeCursor(readBuffer_)) {}

  ~FrameLengthParserStrategyBase() {
    if (frameLengthAndFieldSize_) {
      owner_.decMemoryUsage(static_cast<uint32_t>(frameLengthAndFieldSize_));
    }
  }

  template <bool resize>
  FOLLY_ALWAYS_INLINE void drainReadBuffer() {
    while (size_ >= Serializer::kBytesForFrameOrMetadataLength) {
      if (!frameLength_) {
        computeFrameLength();

        if (UNLIKELY(!owner_.incMemoryUsage(
                static_cast<uint32_t>(frameLengthAndFieldSize_)))) {
          // A rejecting owner is responsible for stopping further reads.
          frameLengthAndFieldSize_ = 0;
          return;
        }

        if constexpr (resize) {
          tryResize();
        }
      }

      if (size_ < frameLengthAndFieldSize_) {
        return;
      }

      readBuffer_.trimStart(Serializer::kBytesForFrameOrMetadataLength);
      auto frame = readBuffer_.split(frameLength_);

      SCOPE_EXIT {
        resetFrameLength();
      };

      if constexpr (std::is_same_v<Buffer, folly::IOBuf>) {
        if (UNLIKELY(copyFrames_)) {
          auto copy = folly::IOBuf::create(frameLength_);
          folly::io::Cursor(frame.get())
              .pull(copy->writableTail(), frameLength_);
          copy->append(frameLength_);
          frame = std::move(copy);
        }
      }

      owner_.handleFrame(std::move(frame));
    }
  }

 private:
  static Storage makeReadBuffer() {
    if constexpr (std::is_same_v<Buffer, folly::IOBuf>) {
      return Storage{folly::IOBufQueue::cacheChainLength()};
    } else {
      return Storage{};
    }
  }

  static Cursor makeCursor(Storage& storage) {
    if constexpr (std::is_same_v<Cursor, folly::io::Cursor>) {
      return Cursor{Traits::cursorBuffer(storage)};
    } else {
      return Cursor{*Traits::cursorBuffer(storage)};
    }
  }

  FOLLY_ALWAYS_INLINE void computeFrameLength() {
    cursor_.reset(Traits::cursorBuffer(readBuffer_));
    if constexpr (std::is_same_v<Cursor, folly::io::Cursor>) {
      frameLength_ = readFrameOrMetadataSize(cursor_);
    } else {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
      std::array<uint8_t, Serializer::kBytesForFrameOrMetadataLength> bytes;
      cursor_.pull(bytes.data(), bytes.size());
      frameLength_ = readFrameOrMetadataSize(bytes);
    }
    frameLengthAndFieldSize_ =
        frameLength_ + Serializer::kBytesForFrameOrMetadataLength;
  }

  FOLLY_ALWAYS_INLINE void resetFrameLength() {
    owner_.decMemoryUsage(static_cast<uint32_t>(frameLengthAndFieldSize_));
    size_ -= frameLengthAndFieldSize_;
    if (size_ == 0) {
      copyFrames_ = false;
    }
    frameLength_ = 0;
    frameLengthAndFieldSize_ = 0;
  }

  FOLLY_ALWAYS_INLINE void tryResize() {
    if (readBuffer_.tailroom() < frameLength_) {
      const auto max = std::max(frameLengthAndFieldSize_, maxBufferSize_);
      readBuffer_.preallocate(minBufferSize_, max, max);
    }
  }

 protected:
  T& owner_;
  size_t size_{0};
  size_t frameLength_{0};
  size_t frameLengthAndFieldSize_{0};
  size_t minBufferSize_;
  size_t maxBufferSize_;
  bool copyFrames_{false};
  Storage readBuffer_;
  Cursor cursor_;
};

template <class T>
class FrameLengthParserStrategy<T, std::unique_ptr<folly::IOBuf>>
    : public FrameLengthParserStrategyBase<T, folly::io::Cursor> {
 public:
  using Base = FrameLengthParserStrategyBase<T, folly::io::Cursor>;

  explicit FrameLengthParserStrategy(
      T& owner, size_t minBufferSize = 256, size_t maxBufferSize = 4096)
      : Base(owner, minBufferSize, maxBufferSize) {}

  void getReadBuffer(void** bufReturn, size_t* lenReturn) {
    auto& readBuffer = readBuffer_;
    const auto tail = readBuffer.tailroom();
    if (tail < Serializer::kBytesForFrameOrMetadataLength) {
      const auto ret = readBuffer.preallocate(minBufferSize_, maxBufferSize_);
      *bufReturn = ret.first;
      *lenReturn = ret.second;
    } else {
      *bufReturn = readBuffer.writableTail();
      *lenReturn = tail;
    }
  }

  void readDataAvailable(size_t len) {
    size_ += len;
    readBuffer_.postallocate(len);
    this->template drainReadBuffer<true>();
  }

  void readBufferAvailable(std::unique_ptr<folly::IOBuf> buffer) {
    size_ += buffer->computeChainDataLength();
    readBuffer_.append(std::move(buffer), true, true);
    this->template drainReadBuffer<false>();
  }

  void setBuffersScarce(bool scarce) { copyFrames_ |= scarce; }
  bool isBufferMovable() { return true; }

 private:
  using Base::copyFrames_;
  using Base::maxBufferSize_;
  using Base::minBufferSize_;
  using Base::readBuffer_;
  using Base::size_;
};

template <class T>
class FrameLengthParserStrategy<T, IOBufChain>
    : public FrameLengthParserStrategyBase<T, io::IOBufChainCursor> {
 public:
  using Base = FrameLengthParserStrategyBase<T, io::IOBufChainCursor>;

  explicit FrameLengthParserStrategy(
      T& owner, size_t minBufferSize = 256, size_t maxBufferSize = 4096)
      : Base(owner, minBufferSize, maxBufferSize),
        estimatedBufLength_(
            std::min(maxBufferSize, Serializer::kMaxFrameOrMetadataLength)) {}

  /*
   * All users of the IOBufChain specialization will only use the
   * readBufferAvailable API. The readDataAvailable API will not be implemented.
   */
  void getReadBuffer(void**, size_t*) {
    XLOG(FATAL) << "getReadBuffer() not supported";
  }

  void readDataAvailable(size_t) {
    XLOG(FATAL) << "readDataAvailable() not supported";
  }

  void readBufferAvailable(std::unique_ptr<folly::IOBuf> buffer) {
    const auto length = buffer->computeChainDataLength();
    recordBufLength(*buffer);
    const auto remaining =
        frameLengthAndFieldSize_ > size_ ? frameLengthAndFieldSize_ - size_ : 0;
    size_ += length;
    if (remaining == 0) {
      readBuffer_.append(std::move(buffer));
    } else {
      readBuffer_.append(std::move(buffer), estimateFrameBufCount(remaining));
    }
    this->template drainReadBuffer<false>();
    frameBufCountEstimate_ =
        frameLengthAndFieldSize_ == 0 ? 0 : estimateFrameBufCount(frameLength_);
  }

  bool isBufferMovable() const noexcept { return true; }

  size_t getFrameCapacity() const noexcept { return readBuffer_.capacity(); }
  size_t getFrameBufCountEstimate() const noexcept {
    return frameBufCountEstimate_;
  }

 private:
  using Base::frameLength_;
  using Base::frameLengthAndFieldSize_;
  using Base::readBuffer_;
  using Base::size_;

  size_t estimateFrameBufCount(size_t remainingBytes) const noexcept {
    constexpr size_t kMinBufCount = 10;
    constexpr size_t kMaxBufCount = 4096;
    const auto expectedBufLength = std::max<size_t>(1, estimatedBufLength_);
    const auto estimate = remainingBytes / expectedBufLength +
        static_cast<size_t>(remainingBytes % expectedBufLength != 0);
    return std::clamp(estimate, kMinBufCount, kMaxBufCount);
  }

  void recordBufLength(size_t length) noexcept {
    constexpr size_t kWeightDenominator = 8;
    const auto sample = std::min(length, Serializer::kMaxFrameOrMetadataLength);
    estimatedBufLength_ =
        (estimatedBufLength_ * (kWeightDenominator - 1) + sample) /
        kWeightDenominator;
  }

  void recordBufLength(const folly::IOBuf& buf) noexcept {
    for (const auto range : buf) {
      recordBufLength(range.size());
    }
  }

  size_t estimatedBufLength_;
  size_t frameBufCountEstimate_{0};
};

} // namespace apache::thrift::rocket
