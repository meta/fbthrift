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
#include <type_traits>

#include <folly/io/Cursor.h>
#include <thrift/lib/cpp2/IOBufChain.h>
#include <thrift/lib/cpp2/transport/rocket/Types.h>
#include <thrift/lib/cpp2/transport/rocket/framing/Serializer.h>

namespace apache::thrift::rocket::test {

folly::StringPiece getRange(folly::IOBuf& iobuf);
folly::StringPiece getRange(const folly::IOBuf& iobuf);

std::pair<std::unique_ptr<folly::IOBuf>, std::unique_ptr<folly::IOBuf>>
splitMetadataAndData(const Payload& p);

template <typename Buffer>
Buffer makeFrameBuffer(
    std::unique_ptr<folly::IOBuf> bytes, size_t chunkSize = 1) {
  Buffer result;
  folly::io::Cursor cursor(bytes.get());
  auto remaining = cursor.totalLength();
  while (remaining) {
    auto chunk = folly::IOBuf::create(0);
    const auto length = std::min(chunkSize, remaining);
    cursor.clone(*chunk, length);
    remaining -= length;
    if constexpr (std::is_same_v<Buffer, IOBufChain>) {
      result.append(std::move(chunk));
    } else if (result) {
      result->appendToChain(std::move(chunk));
    } else {
      result = std::move(chunk);
    }
  }
  return result;
}

template <typename Buffer, typename Frame>
Frame roundTripFrame(Frame frame) {
  Serializer writer;
  std::move(frame).serialize(writer);
  auto bytes = std::move(writer).move();
  bytes->trimStart(Serializer::kBytesForFrameOrMetadataLength);
  return Frame(makeFrameBuffer<Buffer>(std::move(bytes)));
}

} // namespace apache::thrift::rocket::test
