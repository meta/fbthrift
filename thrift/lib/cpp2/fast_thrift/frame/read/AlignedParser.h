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

#include <array>
#include <cstddef>
#include <cstdint>

#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/lang/Hint.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/transport/Parser.h>

namespace apache::thrift::fast_thrift::frame::read {

/**
 * Parses RSocket frames. A REQUEST_RESPONSE or PAYLOAD frame returns its data
 * in a separate buffer. REQUEST_RESPONSE also places its first binary field on
 * a 16-byte boundary.
 *
 *   struct Request {
 *     1: binary blob;   // alone in its own buffer, on a 16-byte boundary
 *     2: i64 offset;
 *   }
 *
 * Thrift hands over a binary field without copying it. Code that keeps `blob`
 * therefore keeps only the data buffer. PAYLOAD gets the same separation, but
 * no alignment.
 *
 * A binary field stays in one buffer only when the frame is not fragmented.
 * REQUEST_RESPONSE alignment also requires the blob to be the first field and
 * the request to use the binary protocol. The parser checks none of these
 * conditions. Parsing still works if one is false, but the binary field may
 * span buffers or lose alignment.
 *
 * A REQUEST_RESPONSE or PAYLOAD frame keeps its header and metadata in one
 * buffer. Its data, when present, is a second buffer. Every other frame type
 * uses one buffer for the whole frame.
 *
 * The parser controls where data lands. A buffer from the caller would remove
 * that control. The class implements Parser and not MovableBufferParser.
 * Native io_uring receive needs a movable read callback. If this parser is
 * installed directly on an io_uring EventBase, folly aborts the process.
 *
 * The parser may reserve an announced section before its bytes arrive. A peer
 * can keep about maxFrameSize bytes allocated after sending only a header.
 * maxFrameSize limits what a frame may claim, and the parser checks it before
 * it allocates anything.
 */
class AlignedParser {
 public:
  // No frame on the wire can be bigger, so by default the cap turns nothing
  // away. A caller that wants a real cap passes the largest frame its service
  // expects.
  static constexpr size_t kDefaultMaxFrameSize = kMaxFrameLength;

  explicit AlignedParser(size_t maxFrameSize = kDefaultMaxFrameSize) noexcept;

  // Offers room through the end of the current frame section, never into the
  // next section or frame. One consume call sends at most one frame to the
  // sink.
  void getReadBuffer(void** bufReturn, size_t* lenReturn) noexcept;

  // Returns Error when the frame is malformed. After that the parser stays on
  // the bad frame, and getReadBuffer offers 0 bytes of room. Call reset()
  // before feeding more bytes, or close the connection.
  template <typename Sink>
  channel_pipeline::Result consume(size_t len, Sink&& sink) noexcept;

  // The factory must return one unshared, empty buffer with at least the
  // requested tailroom. Any other result makes the current frame unreadable,
  // and getReadBuffer then offers no room. An aligned request may use up to 15
  // bytes of that room as headroom.
  void setIOBufFactory(folly::IOBufFactory* factory) noexcept;

  template <typename Sink>
  channel_pipeline::Result resume(Sink&&) noexcept {
    return channel_pipeline::Result::Success;
  }

  void reset() noexcept;

 private:
  enum class State {
    AwaitingHeader,
    // The three below run only for frames that get their own buffers.
    AwaitingMetadataLength,
    AwaitingMetadata,
    AwaitingData,
    // Everything else lands here and is read into one queue.
    AwaitingBody,
    // No more input is accepted until reset.
    Error,
  };

  // What a byte handler tells consume to do next. This exists so the handlers
  // can live in the .cpp. Only the call to the sink has to be a template.
  enum class Step {
    NeedMore,
    FrameReady,
    Bad,
  };

  // One per state, so getReadBuffer is nothing but a switch.
  void headerReadBuffer(void** bufReturn, size_t* lenReturn) noexcept;
  void metadataReadBuffer(void** bufReturn, size_t* lenReturn) noexcept;
  void dataReadBuffer(void** bufReturn, size_t* lenReturn) noexcept;
  void bodyReadBuffer(void** bufReturn, size_t* lenReturn) noexcept;

  Step onBytes(size_t len) noexcept;
  Step onHeaderBytes(size_t len) noexcept;
  Step onMetadataLengthBytes(size_t len) noexcept;
  Step onMetadataBytes(size_t len) noexcept;
  Step onDataBytes(size_t len) noexcept;
  Step onBodyBytes(size_t len) noexcept;

  bool hasOwnBuffers() const noexcept;
  bool needsAlignment() const noexcept;

  Step startOwnBuffers(uint16_t flags) noexcept;
  channel_pipeline::BytesPtr newBuffer(size_t capacity) noexcept;
  bool startHeaderAndMetadata() noexcept;
  bool startBody() noexcept;
  channel_pipeline::BytesPtr newDataBuffer() noexcept;
  channel_pipeline::BytesPtr takeFrame() noexcept;
  void startNextFrame() noexcept;

  static constexpr size_t kAlignment = 16;
  // The 3-byte wire length prefix plus the base header. Every frame starts
  // with these, and only the base header goes downstream.
  static constexpr size_t kHeaderSize = kMetadataLengthSize + kBaseHeaderSize;
  // Three more bytes for the metadata length when a frame carries metadata.
  static constexpr size_t kHeaderWithMetadataSize =
      kHeaderSize + kMetadataLengthSize;

  // Binary protocol puts 10 bytes in front of the blob's own bytes: 3 for the
  // field header of the struct Thrift builds around the RPC arguments, 3 for
  // the field header of the blob, and 4 for the blob length.
  static constexpr size_t kBytesBeforeFirstField = 3 + 3 + 4;

  const size_t maxFrameSize_;

  State state_{State::AwaitingHeader};
  FrameType frameType_{FrameType::RESERVED};
  size_t remainingHeader_;
  size_t remainingMetadata_{0};
  size_t remainingData_{0};
  size_t remainingBody_{0};
  size_t frameLength_{0};

  // The wire length prefix and base header arrive here. If metadata is present,
  // the next three bytes hold its length.
  std::array<uint8_t, kHeaderWithMetadataSize> headerBytes_{};
  size_t headerBytesRead_{0};

  // REQUEST_RESPONSE and PAYLOAD keep their header and metadata together. Data
  // is the second node. Every other frame is read into one buffer in body_.
  channel_pipeline::BytesPtr headerAndMetadata_;
  channel_pipeline::BytesPtr data_;
  folly::IOBufQueue body_{folly::IOBufQueue::cacheChainLength()};

  folly::IOBufFactory* bufFactory_{nullptr};
};

template <typename Sink>
channel_pipeline::Result AlignedParser::consume(
    size_t len, Sink&& sink) noexcept {
  const Step step = onBytes(len);
  if (step == Step::NeedMore) {
    return channel_pipeline::Result::Success;
  }
  if (step == Step::Bad) [[unlikely]] {
    state_ = State::Error;
    return channel_pipeline::Result::Error;
  }
  return sink(takeFrame());
}

static_assert(transport::Parser<AlignedParser>);
static_assert(!transport::MovableBufferParser<AlignedParser>);

} // namespace apache::thrift::fast_thrift::frame::read
