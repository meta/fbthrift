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

#include <folly/Portability.h>

#include <glog/logging.h>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace apache::thrift::fast_thrift::frame::read {

// ── Index policies (stateless, zero-cost) ──────────────────────────────────

/// For RSocket stream IDs (uint32_t, stride +2).
/// (key >> 1) maps odd-only or even-only IDs to consecutive slots.
struct StreamIdIndex {
  template <typename K>
  static size_t apply(K key, size_t mask) noexcept {
    return (key >> 1) & mask;
  }
};

/// For sequential IDs (uint64_t request IDs, stride +1).
/// Identity mask — consecutive keys land in consecutive slots.
struct SequentialIndex {
  template <typename K>
  static size_t apply(K key, size_t mask) noexcept {
    return static_cast<size_t>(key) & mask;
  }
};

// ── DirectStreamMap ────────────────────────────────────────────────────────

/**
 * DirectStreamMap - Direct-mapped open-addressing table for integer keys.
 *
 * Template parameters:
 *   Value       - mapped type (must be move-constructible and
 *                 default-constructible)
 *   Key         - key type (default: uint32_t for RSocket stream IDs)
 *   IndexPolicy - static policy with `size_t apply(Key, size_t mask)` that
 *                 maps a key to an initial slot index.
 *                 Default: StreamIdIndex  ((key >> 1) & mask)
 *
 * RSocket stream IDs increment by 2 (odd=client, even=server), so >> 1
 * produces sequential indices with zero collisions for same-parity traffic.
 * For sequential +1 keys (e.g. request IDs), use SequentialIndex.
 *
 * Linear probe on the rare collision. Backshift delete (Robin Hood style) —
 * no tombstones, find() always stops at the first empty slot.
 */
template <
    typename Value,
    typename Key = uint32_t,
    typename IndexPolicy = StreamIdIndex>
class DirectStreamMap {
  static constexpr size_t kInitialCapacity = 16;
  static constexpr size_t kLoadNum = 7;
  static constexpr size_t kLoadDen = 8;
  static constexpr uint8_t kShrinkAfterLowUtilizationDrains = 8;
  static constexpr uint8_t kShrinkProbeBit = 0x80;
  static constexpr uint8_t kShrinkDrainMask = 0x7f;
  static constexpr uint8_t kShrinkDisabled = 0xff;

  enum class Tag : uint8_t { Empty, Live };

 public:
  struct Slot {
    Key first{0};
    Value second{};
    Tag tag{Tag::Empty};
  };

  using iterator = Slot*;
  using const_iterator = const Slot*;

  DirectStreamMap() = default;

  explicit DirectStreamMap(size_t cap) : slots_(nextPow2(cap)) {}

  ~DirectStreamMap() = default;

  DirectStreamMap(const DirectStreamMap&) = delete;
  DirectStreamMap& operator=(const DirectStreamMap&) = delete;

  DirectStreamMap(DirectStreamMap&& other) noexcept
      : slots_(std::move(other.slots_)),
        size_(std::exchange(other.size_, 0)),
        displacedCount_(std::exchange(other.displacedCount_, 0)),
        shrinkState_(std::exchange(other.shrinkState_, 0)) {}

  DirectStreamMap& operator=(DirectStreamMap&& other) noexcept {
    if (this != &other) {
      slots_ = std::move(other.slots_);
      size_ = std::exchange(other.size_, 0);
      displacedCount_ = std::exchange(other.displacedCount_, 0);
      shrinkState_ = std::exchange(other.shrinkState_, 0);
    }
    return *this;
  }

  iterator find(Key key) noexcept {
    if (slots_.empty()) [[unlikely]] {
      return end();
    }
    auto m = mask();
    auto idx = IndexPolicy::apply(key, m);
    for (size_t i = 0; i < slots_.size(); ++i) {
      auto& s = slots_[idx];
      if (s.tag == Tag::Empty) {
        return nullptr;
      }
      if (s.first == key) {
        return &s;
      }
      idx = (idx + 1) & m;
    }
    return nullptr;
  }

  const_iterator find(Key key) const noexcept {
    return const_cast<DirectStreamMap*>(this)->find(key);
  }

  iterator end() noexcept { return nullptr; }
  const_iterator end() const noexcept { return nullptr; }

  template <typename... Args>
  std::pair<iterator, bool> emplace(Key key, Args&&... args) {
    if (size_ * kLoadDen >= slots_.size() * kLoadNum) [[unlikely]] {
      grow();
    }
    return insertInternal(key, std::forward<Args>(args)...);
  }

  // All mutations invalidate iterators. Passing an invalidated iterator is
  // undefined behavior, even if its address now refers to another live slot.
  void erase(iterator it) noexcept {
    if (!it) {
      return;
    }
    eraseSlot(static_cast<size_t>(it - slots_.data()));
  }

  void erase(Key key) noexcept {
    auto it = find(key);
    if (it) {
      eraseSlot(static_cast<size_t>(it - slots_.data()));
    }
  }

  /// Iterate all live entries. Callback signature: void(Key key, Value& val)
  template <typename F>
  void forEach(F&& fn) noexcept {
    for (auto& s : slots_) {
      if (s.tag == Tag::Live) {
        fn(s.first, s.second);
      }
    }
  }

  /// Return the first live entry matching the predicate.
  /// Predicate signature: bool(Key key, Value& val)
  template <typename F>
  iterator findIf(F&& fn) noexcept {
    for (auto& s : slots_) {
      if (s.tag == Tag::Live && fn(s.first, s.second)) {
        return &s;
      }
    }
    return end();
  }

  bool contains(Key key) const noexcept { return find(key) != nullptr; }

  bool empty() const noexcept { return size_ == 0; }
  size_t size() const noexcept { return size_; }
  size_t capacity() const noexcept { return slots_.size(); }

  void clear() noexcept {
    if (empty()) {
      return;
    }
    for (auto& s : slots_) {
      s.tag = Tag::Empty;
      s.first = Key{0};
      s.second = Value{};
    }
    size_ = 0;
    displacedCount_ = 0;
    maybeShrink();
  }

 private:
  size_t mask() const noexcept { return slots_.size() - 1; }

  static size_t nextPow2(size_t n) noexcept {
    if (n == 0) {
      return 0;
    }
    if (n <= kInitialCapacity) {
      return kInitialCapacity;
    }
    return std::bit_ceil(n);
  }

  // Circular displacement check: is 'gap' between 'nat' and 'cur'?
  // If so, the entry at 'cur' was displaced past 'gap' and should
  // be shifted back.
  static bool displaced(size_t nat, size_t gap, size_t cur) noexcept {
    if (nat <= gap) {
      return gap < cur || cur < nat;
    }
    return gap < cur && cur < nat;
  }

  void eraseSlot(size_t idx) noexcept {
    CHECK_EQ(
        static_cast<unsigned>(slots_[idx].tag),
        static_cast<unsigned>(Tag::Live))
        << "DirectStreamMap::erase called with a stale iterator";
    const auto m = mask();
    if (IndexPolicy::apply(slots_[idx].first, m) != idx) {
      --displacedCount_;
    }
    slots_[idx].tag = Tag::Empty;
    slots_[idx].second = Value{};
    --size_;

    if (displacedCount_ == 0) {
      if (size_ == 0) [[unlikely]] {
        maybeShrink();
      }
      return;
    }

    // Backshift: walk forward, pull displaced entries back to fill the gap.
    auto next = (idx + 1) & m;
    while (slots_[next].tag == Tag::Live) {
      const auto natural = IndexPolicy::apply(slots_[next].first, m);
      if (displaced(natural, idx, next)) {
        slots_[idx] = std::move(slots_[next]);
        slots_[next].tag = Tag::Empty;
        slots_[next].second = Value{};
        if (natural == idx) {
          --displacedCount_;
          if (displacedCount_ == 0) {
            return;
          }
        }
        idx = next;
      }
      next = (next + 1) & m;
    }
  }

  FOLLY_NOINLINE void grow() {
    shrinkState_ =
        shrinkState_ & kShrinkProbeBit ? kShrinkDisabled : uint8_t{0};
    auto old = std::move(slots_);
    const auto newCapacity = old.empty() ? kInitialCapacity : old.size() * 2;
    slots_ = std::vector<Slot>(newCapacity);
    size_ = 0;
    displacedCount_ = 0;
    for (auto& s : old) {
      if (s.tag == Tag::Live) {
        insertInternal(s.first, std::move(s.second));
      }
    }
  }

  void maybeShrink() noexcept {
    if (slots_.size() <= kInitialCapacity || shrinkState_ == kShrinkDisabled) {
      return;
    }
    const auto drainCount =
        static_cast<uint8_t>((shrinkState_ & kShrinkDrainMask) + 1);
    if (drainCount < kShrinkAfterLowUtilizationDrains) {
      shrinkState_ =
          static_cast<uint8_t>((shrinkState_ & kShrinkProbeBit) | drainCount);
      return;
    }
    shrink();
  }

  FOLLY_NOINLINE void shrink() noexcept {
    // Shrinking only follows a full drain, so there are no entries to rehash.
    DCHECK(empty());
    shrinkState_ = 0;
    try {
      std::vector<Slot> smaller(slots_.size() / 2);
      slots_.swap(smaller);
      shrinkState_ = kShrinkProbeBit;
    } catch (...) {
      // Shrinking is best-effort and must not make noexcept erase paths fail.
    }
  }

  template <typename... Args>
  std::pair<iterator, bool> insertInternal(Key key, Args&&... args) {
    const auto m = mask();
    const auto natural = IndexPolicy::apply(key, m);
    auto idx = natural;
    for (size_t i = 0; i < slots_.size(); ++i) {
      auto& s = slots_[idx];
      if (s.tag == Tag::Empty) {
        s.first = key;
        s.tag = Tag::Live;
        s.second = Value(std::forward<Args>(args)...);
        ++size_;
        displacedCount_ += idx != natural;
        return {&s, true};
      }
      if (s.first == key) {
        return {&s, false};
      }
      idx = (idx + 1) & m;
    }
    return {nullptr, false};
  }

  std::vector<Slot> slots_;
  size_t size_{0};
  size_t displacedCount_{0};
  uint8_t shrinkState_{0};
};

// DirectStreamSet — a DirectStreamMap used purely for key membership.
// The value type is uint8_t (minimal, never read).
template <typename Key = uint32_t, typename IndexPolicy = StreamIdIndex>
using DirectStreamSet = DirectStreamMap<uint8_t, Key, IndexPolicy>;

} // namespace apache::thrift::fast_thrift::frame::read
