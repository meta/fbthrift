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

#include <glog/logging.h>
#include <folly/lang/Hint.h>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

/**
 * Records which handlers wait for a write-ready or a read-ready callback, by
 * logical handler index.
 *
 * Every index argument must be from 0 to 63, also the begin or end bound of a
 * scan. So a scan below an end bound never finds index 63. A larger value is
 * undefined behavior, and debug builds check it.
 */
class ReadyWaiters {
 public:
  static constexpr std::size_t kMaxIndexes = 64;

  void arm(std::size_t index) noexcept {
    // When the compiler knows the index, it may write only the byte that holds
    // the bit. If a pass reads the whole word soon after, that read waits until
    // the one-byte write reaches the cache. With the index hidden, the compiler
    // writes the whole word, and the read can use that write at once.
    folly::compiler_must_not_predict(index);
    armed_ |= bit(index);
  }
  void cancel(std::size_t index) noexcept {
    // The index is hidden for the same reason as in arm().
    folly::compiler_must_not_predict(index);
    armed_ &= ~bit(index);
  }
  bool isArmed(std::size_t index) const noexcept {
    return (armed_ & bit(index)) != 0;
  }
  bool empty() const noexcept { return armed_ == 0; }
  void clear() noexcept { armed_ = 0; }

  // Returns the lowest armed index that is begin or higher, or nothing if no
  // such index is armed.
  std::optional<std::size_t> lowestArmedFrom(std::size_t begin) const noexcept {
    const std::uint64_t candidates = armed_ & ~(bit(begin) - 1);
    if (candidates == 0) {
      return std::nullopt;
    }
    return static_cast<std::size_t>(std::countr_zero(candidates));
  }

  // Returns the highest armed index below end, or nothing if no such index is
  // armed.
  std::optional<std::size_t> highestArmedBelow(std::size_t end) const noexcept {
    const std::uint64_t candidates = armed_ & (bit(end) - 1);
    if (candidates == 0) {
      return std::nullopt;
    }
    return kMaxIndexes - 1 -
        static_cast<std::size_t>(std::countl_zero(candidates));
  }

 private:
  static std::uint64_t bit(std::size_t index) noexcept {
    DCHECK_LT(index, kMaxIndexes);
    return std::uint64_t{1} << index;
  }

  std::uint64_t armed_{0};
};

// The write-ready set has a bit for each handler and one more for the head
// endpoint, so a pipeline holds at most 63 handlers.
inline constexpr std::size_t kMaxPipelineHandlers =
    ReadyWaiters::kMaxIndexes - 1;

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
