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
#include <atomic>
#include <string_view>
#include <type_traits>

#include <folly/Portability.h>
#include <folly/Range.h>
#include <folly/Traits.h>
#include <folly/container/F14Map.h>
#include <folly/lang/Align.h>
#include <folly/lang/Exception.h>
#include <folly/synchronization/AtomicUtil.h>

#include <thrift/lib/cpp2/Thrift.h>

namespace apache::thrift::detail::st {

template <typename Int>
struct alignas(folly::cacheline_align_v) enum_find {
  struct find_name_result {
    std::string_view result{};
    find_name_result() = default;
    explicit constexpr find_name_result(std::string_view r) noexcept
        : result{r} {}
    explicit constexpr operator bool() const noexcept {
      return result.data() != nullptr;
    }
  };
  struct find_value_result {
    bool found{false};
    Int result{0};
    find_value_result() = default;
    explicit constexpr find_value_result(Int r) noexcept
        : found{true}, result{r} {}
    explicit constexpr operator bool() const noexcept { return found; }
  };

  // metadata for the slow path to fill the caches for the fast path
  struct metadata {
    std::size_t const size{};
    const Int* const values{};
    const std::string_view* const names{};
  };

  // the fast path cache types
  using find_name_map_t = folly::F14FastMap<Int, std::string_view>;
  using find_value_map_t = folly::F14FastMap<std::string_view, Int>;

  // an approximate state of the fast-path caches; approximately mutex-like
  struct cache_state {
    std::atomic<unsigned> cell{0}; // 0 init, +1 locked, -1 ready
    FOLLY_ERASE bool ready() noexcept {
      return folly::to_signed(cell.load(std::memory_order_acquire)) < 0;
    }
    FOLLY_ERASE bool try_lock() noexcept {
      return !folly::atomic_fetch_set(cell, 0, std::memory_order_relaxed);
    }
    FOLLY_ERASE bool unlock(bool ready) noexcept {
      return cell.store(ready ? -1 : 0, std::memory_order_release), ready;
    }
  };

  // the fast-path caches, guarded by the cache-state
  struct bidi_cache {
    find_name_map_t find_name_index;
    find_value_map_t find_value_index;

    FOLLY_NOINLINE explicit bidi_cache(const metadata& meta_) {
      find_name_index.reserve(meta_.size);
      find_value_index.reserve(meta_.size);
      for (std::size_t i = 0; i < meta_.size; ++i) {
        find_name_index.emplace(meta_.values[i], meta_.names[i].data());
        find_value_index.emplace(meta_.names[i].data(), meta_.values[i]);
      }
    }
  };

  // these fields all fit within a single cache line for fast path performance
  // the metadata is stored separately since it is used only in the slow path
  cache_state state; // protects the fast-path caches
  folly::aligned_storage_for_t<bidi_cache> cache{}; // the fast-path caches
  const metadata& meta; // source for the fast-path caches

  FOLLY_ERASE explicit constexpr enum_find(const metadata& meta_) noexcept
      : meta{meta_} {}

  FOLLY_NOINLINE bool prep_and_unlock() noexcept {
    const auto try_ = [&] { return ::new (&cache) bidi_cache(meta), true; };
    const auto catch_ = []() noexcept { return false; };
    return state.unlock(folly::catch_exception(try_, +catch_));
  }
  FOLLY_ERASE bool try_prepare() noexcept {
    return state.try_lock() && prep_and_unlock();
  }

  FOLLY_ERASE find_name_result find_name_fast(const Int value) noexcept {
    using result = find_name_result;
    const auto& map = reinterpret_cast<bidi_cache&>(cache).find_name_index;
    const auto found = map.find(value);
    return found == map.end() ? result() : result(found->second);
  }
  FOLLY_NOINLINE find_name_result find_name_scan(const Int value) noexcept {
    using result = find_name_result;
    // reverse order to simulate loop-map-insert then map-find
    const auto range = folly::range(meta.values, meta.values + meta.size);
    const auto found = range.rfind(value);
    return found == range.npos ? result() : result(meta.names[found]);
  }
  // param order optimizes outline findName by minimizing native instructions
  FOLLY_NOINLINE static find_name_result find_name(
      const Int value, enum_find& self) noexcept {
    // with two likelinesses v.s. one, gets the right code layout
    return FOLLY_LIKELY(self.state.ready()) || FOLLY_LIKELY(self.try_prepare())
        ? self.find_name_fast(value)
        : self.find_name_scan(value);
  }

  FOLLY_ERASE find_value_result
  find_value_fast(std::string_view const name) noexcept {
    using result = find_value_result;
    const auto& map = reinterpret_cast<bidi_cache&>(cache).find_value_index;
    const auto found = map.find(name);
    return found == map.end() ? result() : result(found->second);
  }
  FOLLY_NOINLINE find_value_result
  find_value_scan(std::string_view const name) noexcept {
    using result = find_value_result;
    // reverse order to simulate loop-map-insert then map-find
    const auto range = folly::range(meta.names, meta.names + meta.size);
    const auto found = range.rfind(name);
    return found == range.npos ? result() : result(meta.values[found]);
  }
  // param order optimizes outline findValue by minimizing native instructions
  FOLLY_NOINLINE static find_value_result find_value(
      std::string_view const name, enum_find& self) noexcept {
    // with two likelinesses v.s. one, gets the right code layout
    return FOLLY_LIKELY(self.state.ready()) || FOLLY_LIKELY(self.try_prepare())
        ? self.find_value_fast(name)
        : self.find_value_scan(name);
  }
};

template <typename E, typename U>
typename enum_find<U>::metadata enum_find_make_metadata() {
  using traits = TEnumTraits<E>;
  return {
      traits::size,
      reinterpret_cast<const U*>(traits::values.data()),
      traits::names.data()};
}

template <typename E, typename U = std::underlying_type_t<E>>
FOLLY_EXPORT FOLLY_ALWAYS_INLINE enum_find<U>& enum_find_instance() {
  static const auto meta = enum_find_make_metadata<E, U>();
  static enum_find<U> impl{meta};
  return impl;
}

template <typename E>
consteval auto enum_find_name_dense_array_make() {
  using T = TEnumTraits<E>;
  using D = TEnumDataStorage<E>;
  constexpr auto min = folly::to_underlying(T::min());
  constexpr auto max = folly::to_underlying(T::max());
  std::array<int16_t, max - min + 1> ret;
  for (auto& e : ret) {
    e = -1;
  }
  for (size_t i = 0; i < T::size; ++i) {
    ret[folly::to_underlying(D::values[i]) - min] = i;
  }
  return ret;
}

template <typename E>
inline constexpr auto enum_find_name_dense =
    enum_find_name_dense_array_make<E>();

template <typename E, typename U = std::underlying_type_t<E>>
FOLLY_ERASE bool enum_find_name(
    E const value, std::string_view* const out) noexcept {
  constexpr auto sz = TEnumTraits<E>::size;
  // excludes empty enums and empty union tag enums, but these are typically
  // unlikely to be searched anyway
  if constexpr (sz && sz <= 4096) {
    constexpr auto min = folly::to_underlying(TEnumTraits<E>::min());
    constexpr auto max = folly::to_underlying(TEnumTraits<E>::max());
    constexpr auto gap = static_cast<int64_t>(max) - static_cast<int64_t>(min);
    if constexpr (gap < 2 * sz || gap < 16) {
      constexpr auto& index = enum_find_name_dense<E>;
      const auto under = folly::to_underlying(value);
      const auto idx = under < min || under > max ? -1 : index[under - min];
      return idx >= 0 && ((*out = TEnumDataStorage<E>::names[idx]), true);
    }
  }
  const auto r = enum_find<U>::find_name(U(value), enum_find_instance<E>());
  return r && ((*out = r.result), true);
}

template <typename E, typename U = std::underlying_type_t<E>>
FOLLY_ERASE bool enum_find_value(
    std::string_view const name, E* const out) noexcept {
  const auto uout = reinterpret_cast<U*>(out);
  const auto r = enum_find<U>::find_value(name, enum_find_instance<E>());
  return r && ((*uout = r.result), true);
}

extern template struct enum_find<int>;

} // namespace apache::thrift::detail::st
