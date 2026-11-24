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

/**
 * Compares inbound TransportHandler paths for both frame parsers.
 *
 * The first group models a TLS connection after Fizz decrypts application
 * data. Most cases pass one frame per input buffer. The batched case passes 64
 * frames in one buffer. Fizz uses readBufferAvailable for FrameLengthParser
 * and copies the same input through buffers supplied by AlignedParser.
 *
 * The second group sends both parsers through getReadBuffer and
 * readDataAvailable, like a plaintext AsyncSocket. It also reports the number
 * of getReadBuffer calls per million frames. The timing does not include
 * system calls.
 *
 * The median of three @fbcode//mode/opt-clang-lto runs on 2026-09-25 was:
 *   - TLS, REQUEST_RESPONSE with metadata, 100 B: 49.80 ns for
 *     FrameLengthParser and 143.27 ns for AlignedParser;
 *   - TLS, the same frame in batches of 64: 35.15 ns and 123.32 ns;
 *   - TLS, REQUEST_RESPONSE with metadata, 1 KiB: 70.38 ns and 196.94 ns;
 *   - TLS, REQUEST_RESPONSE with metadata, 64 KiB: 263.85 ns and 2.41 us;
 *   - plaintext REQUEST_RESPONSE with metadata, 100 B: 40.89 ns and 42,968
 *     calls per million frames for FrameLengthParser, 127.30 ns and 4,000,000
 *     calls for AlignedParser;
 *   - plaintext REQUEST_RESPONSE without metadata, 100 B: 37.82 ns and 26,611
 *     calls for FrameLengthParser, 96.20 ns and 2,000,000 calls for
 *     AlignedParser;
 *   - plaintext REQUEST_FNF, 100 B: 37.81 ns and 26,611 calls for
 *     FrameLengthParser, 76.40 ns and 2,000,000 calls for AlignedParser.
 *
 * The TLS arms do different work. FrameLengthParser takes over an input buffer
 * built before the timer starts. AlignedParser allocates its output buffers
 * inside the timer and copies the input into them.
 */

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

#include <glog/logging.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/AlignedParser.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameLengthParser.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameHeaders.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameLength.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameWriter.h>
#include <thrift/lib/cpp2/fast_thrift/transport/TransportHandler.h>
#include <thrift/lib/cpp2/fast_thrift/transport/bench/BenchAsyncTransport.h>

using apache::thrift::fast_thrift::channel_pipeline::BytesPtr;
using apache::thrift::fast_thrift::channel_pipeline::PipelineOwner;
using apache::thrift::fast_thrift::channel_pipeline::Result;
using apache::thrift::fast_thrift::channel_pipeline::SimpleBufferAllocator;
using apache::thrift::fast_thrift::channel_pipeline::StaticPipelineBuilder;
using apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox;
using apache::thrift::fast_thrift::frame::kMetadataLengthSize;
using apache::thrift::fast_thrift::frame::read::AlignedParser;
using apache::thrift::fast_thrift::frame::read::FrameLengthParser;
using apache::thrift::fast_thrift::frame::write::RequestFnfHeader;
using apache::thrift::fast_thrift::frame::write::RequestResponseHeader;
using apache::thrift::fast_thrift::frame::write::serialize;
using apache::thrift::fast_thrift::frame::write::writeFrameLength;
using apache::thrift::fast_thrift::transport::NoOpWriteCompleteEventFactory;
using apache::thrift::fast_thrift::transport::TransportHandlerT;
using apache::thrift::fast_thrift::transport::bench::BenchAsyncTransport;

namespace {

constexpr size_t kMetadataSize = 64;
constexpr size_t kSmallDataSize = 100;
constexpr size_t kMediumDataSize = 1024;
constexpr size_t kLargeDataSize = 64 * 1024;
constexpr size_t kFramesPerBatch = 64;

BytesPtr filledBuffer(size_t size, uint8_t value) {
  BytesPtr buffer = folly::IOBuf::create(size);
  std::memset(buffer->writableData(), value, size);
  buffer->append(size);
  return buffer;
}

std::vector<uint8_t> buildRequestResponseFrame(
    size_t dataSize, bool withMetadata = true) {
  BytesPtr metadata;
  if (withMetadata) {
    metadata = filledBuffer(kMetadataSize, 'm');
  }
  BytesPtr frame = serialize(
      RequestResponseHeader{.streamId = 1},
      std::move(metadata),
      filledBuffer(dataSize, 'd'));
  const folly::ByteRange body = frame->coalesce();

  std::vector<uint8_t> bytes(kMetadataLengthSize + body.size());
  writeFrameLength(bytes.data(), body.size());
  std::memcpy(bytes.data() + kMetadataLengthSize, body.data(), body.size());
  return bytes;
}

std::vector<uint8_t> buildRequestFnfFrame(size_t dataSize) {
  BytesPtr frame = serialize(
      RequestFnfHeader{.streamId = 1}, nullptr, filledBuffer(dataSize, 'd'));
  const folly::ByteRange body = frame->coalesce();

  std::vector<uint8_t> bytes(kMetadataLengthSize + body.size());
  writeFrameLength(bytes.data(), body.size());
  std::memcpy(bytes.data() + kMetadataLengthSize, body.data(), body.size());
  return bytes;
}

class FrameSink {
 public:
  template <typename Context>
  Result onRead(Context&, TypeErasedBox&& message) noexcept {
    BytesPtr frame = std::move(message.get<BytesPtr>());
    folly::doNotOptimizeAway(frame.get());
    ++framesRead_;
    return Result::Success;
  }

  void onException(folly::exception_wrapper&&) noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}
  void onWriteReady() noexcept {}

  size_t framesRead() const noexcept { return framesRead_; }

 private:
  size_t framesRead_{0};
};

template <typename ParserT>
class HandlerFixture {
 private:
  using TransportHandler =
      TransportHandlerT<NoOpWriteCompleteEventFactory, ParserT>;

 public:
  HandlerFixture() = default;
  HandlerFixture(const HandlerFixture&) = delete;
  HandlerFixture& operator=(const HandlerFixture&) = delete;
  HandlerFixture(HandlerFixture&&) = delete;
  HandlerFixture& operator=(HandlerFixture&&) = delete;

  ~HandlerFixture() {
    if (transportHandler_) {
      transportHandler_->close(folly::exception_wrapper{});
      transportHandler_->resetPipeline();
    }
    pipeline_.reset();
  }

  void setup() {
    folly::AsyncTransport::UniquePtr transport(
        new BenchAsyncTransport(&eventBase_));
    testTransport_ = static_cast<BenchAsyncTransport*>(transport.get());
    transportHandler_ =
        TransportHandler::createWithParser(std::move(transport), ParserT{});

    pipeline_ = StaticPipelineBuilder<
                    TransportHandler,
                    FrameSink,
                    SimpleBufferAllocator>()
                    .setEventBase(&eventBase_)
                    .setHead(transportHandler_.get())
                    .setTail(&frameSink_)
                    .setAllocator(&allocator_)
                    .build();

    transportHandler_->setPipeline(pipeline_.get());
    transportHandler_->onConnect();
  }

  void inject(BytesPtr frame) {
    testTransport_->injectReadData(std::move(frame));
  }

  void runEventLoopOnce() { eventBase_.loopOnce(EVLOOP_NONBLOCK); }

  size_t injectThroughReadBuffer(BytesPtr data) {
    folly::io::Cursor cursor(data.get());
    size_t readCalls = 0;
    while (cursor.totalLength() > 0) {
      folly::AsyncTransport::ReadCallback* callback =
          CHECK_NOTNULL(testTransport_->getReadCallback());
      void* buffer = nullptr;
      size_t room = 0;
      callback->getReadBuffer(&buffer, &room);
      CHECK_NOTNULL(buffer);
      CHECK_GT(room, 0);
      const size_t length = std::min(room, cursor.totalLength());
      cursor.pull(buffer, length);
      callback->readDataAvailable(length);
      ++readCalls;
    }
    return readCalls;
  }

  size_t framesRead() const noexcept { return frameSink_.framesRead(); }

 private:
  folly::EventBase eventBase_;
  SimpleBufferAllocator allocator_;
  FrameSink frameSink_;
  BenchAsyncTransport* testTransport_{nullptr};
  typename TransportHandler::Ptr transportHandler_;
  PipelineOwner pipeline_;
};

struct BenchmarkFrames {
  std::vector<uint8_t> small;
  std::vector<uint8_t> smallWithoutMetadata;
  std::vector<uint8_t> medium;
  std::vector<uint8_t> large;
  std::vector<uint8_t> plainSmall;
};

const BenchmarkFrames& benchmarkFrames() {
  static const BenchmarkFrames frames{
      buildRequestResponseFrame(kSmallDataSize),
      buildRequestResponseFrame(kSmallDataSize, false),
      buildRequestResponseFrame(kMediumDataSize),
      buildRequestResponseFrame(kLargeDataSize),
      buildRequestFnfFrame(kSmallDataSize),
  };
  return frames;
}

template <typename ParserT>
void runHandlerBenchmark(
    size_t iterations,
    const std::vector<uint8_t>& wireFrame,
    size_t framesPerBuffer = 1) {
  folly::BenchmarkSuspender suspender;
  HandlerFixture<ParserT> fixture;
  fixture.setup();

  std::vector<BytesPtr> buffers;
  buffers.reserve((iterations + framesPerBuffer - 1) / framesPerBuffer);
  for (size_t first = 0; first < iterations; first += framesPerBuffer) {
    const size_t frameCount = std::min(framesPerBuffer, iterations - first);
    BytesPtr buffer = folly::IOBuf::create(frameCount * wireFrame.size());
    for (size_t i = 0; i < frameCount; ++i) {
      std::memcpy(buffer->writableTail(), wireFrame.data(), wireFrame.size());
      buffer->append(wireFrame.size());
    }
    buffers.push_back(std::move(buffer));
  }

  suspender.dismiss();

  for (BytesPtr& buffer : buffers) {
    fixture.inject(std::move(buffer));
  }
  fixture.runEventLoopOnce();

  suspender.rehire();
  CHECK_EQ(fixture.framesRead(), iterations);
}

template <typename ParserT>
void runPlaintextBenchmark(
    folly::UserCounters& counters,
    size_t iterations,
    const std::vector<uint8_t>& wireFrame) {
  folly::BenchmarkSuspender suspender;
  HandlerFixture<ParserT> fixture;
  fixture.setup();

  std::vector<uint8_t> bytes;
  bytes.reserve(iterations * wireFrame.size());
  for (size_t i = 0; i < iterations; ++i) {
    bytes.insert(bytes.end(), wireFrame.begin(), wireFrame.end());
  }
  BytesPtr frames = folly::IOBuf::copyBuffer(bytes.data(), bytes.size());

  suspender.dismiss();
  const size_t readCalls = fixture.injectThroughReadBuffer(std::move(frames));
  fixture.runEventLoopOnce();
  suspender.rehire();

  CHECK_EQ(fixture.framesRead(), iterations);
  counters["getReadBuffer_calls/1M_frames"] = folly::UserMetric(
      1000000.0 * static_cast<double>(readCalls) /
      static_cast<double>(iterations));
}

} // namespace

BENCHMARK(FrameLengthParser_Handler_Small, n) {
  runHandlerBenchmark<FrameLengthParser>(n, benchmarkFrames().small);
}

BENCHMARK_RELATIVE(AlignedParser_Handler_Small, n) {
  runHandlerBenchmark<AlignedParser>(n, benchmarkFrames().small);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(FrameLengthParser_Handler_BatchedSmall, n) {
  runHandlerBenchmark<FrameLengthParser>(
      n, benchmarkFrames().small, kFramesPerBatch);
}

BENCHMARK_RELATIVE(AlignedParser_Handler_BatchedSmall, n) {
  runHandlerBenchmark<AlignedParser>(
      n, benchmarkFrames().small, kFramesPerBatch);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(FrameLengthParser_Handler_Medium, n) {
  runHandlerBenchmark<FrameLengthParser>(n, benchmarkFrames().medium);
}

BENCHMARK_RELATIVE(AlignedParser_Handler_Medium, n) {
  runHandlerBenchmark<AlignedParser>(n, benchmarkFrames().medium);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(FrameLengthParser_Handler_Large, n) {
  runHandlerBenchmark<FrameLengthParser>(n, benchmarkFrames().large);
}

BENCHMARK_RELATIVE(AlignedParser_Handler_Large, n) {
  runHandlerBenchmark<AlignedParser>(n, benchmarkFrames().large);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_COUNTERS(
    FrameLengthParser_Plaintext_RequestResponseWithMetadata_Small,
    counters,
    n) {
  runPlaintextBenchmark<FrameLengthParser>(
      counters, n, benchmarkFrames().small);
}

BENCHMARK_COUNTERS_RELATIVE(
    AlignedParser_Plaintext_RequestResponseWithMetadata_Small, counters, n) {
  runPlaintextBenchmark<AlignedParser>(counters, n, benchmarkFrames().small);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_COUNTERS(
    FrameLengthParser_Plaintext_RequestResponseWithoutMetadata_Small,
    counters,
    n) {
  runPlaintextBenchmark<FrameLengthParser>(
      counters, n, benchmarkFrames().smallWithoutMetadata);
}

BENCHMARK_COUNTERS_RELATIVE(
    AlignedParser_Plaintext_RequestResponseWithoutMetadata_Small, counters, n) {
  runPlaintextBenchmark<AlignedParser>(
      counters, n, benchmarkFrames().smallWithoutMetadata);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_COUNTERS(FrameLengthParser_Plaintext_RequestFnf_Small, counters, n) {
  runPlaintextBenchmark<FrameLengthParser>(
      counters, n, benchmarkFrames().plainSmall);
}

BENCHMARK_COUNTERS_RELATIVE(
    AlignedParser_Plaintext_RequestFnf_Small, counters, n) {
  runPlaintextBenchmark<AlignedParser>(
      counters, n, benchmarkFrames().plainSmall);
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  // Builds the frames now, so no benchmark is timed building them.
  (void)benchmarkFrames();
  folly::runBenchmarks();
  return 0;
}
