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

#include <thrift/lib/cpp2/dynamic/StableCompare.h>

#include <folly/io/Cursor.h>
#include <thrift/lib/cpp2/dynamic/Any.h>
#include <thrift/lib/cpp2/dynamic/Binary.h>
#include <thrift/lib/cpp2/dynamic/List.h>
#include <thrift/lib/cpp2/dynamic/Map.h>
#include <thrift/lib/cpp2/dynamic/Set.h>
#include <thrift/lib/cpp2/dynamic/String.h>
#include <thrift/lib/cpp2/dynamic/Struct.h>
#include <thrift/lib/cpp2/dynamic/Union.h>
#include <thrift/lib/cpp2/op/detail/Compare.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <numeric>
#include <type_traits>

namespace apache::thrift::dynamic::detail {

namespace {

constexpr auto kLess = std::partial_ordering::less;
constexpr auto kGreater = std::partial_ordering::greater;
constexpr auto kEquivalent = std::partial_ordering::equivalent;

// Like folly::IOBufCompare, which needs IOBufs rather than cursors.
std::partial_ordering compareBytes(
    folly::io::Cursor lhs, folly::io::Cursor rhs) {
  while (true) {
    auto lhsBytes = lhs.peekBytes();
    auto rhsBytes = rhs.peekBytes();
    if (lhsBytes.empty() || rhsBytes.empty()) {
      // The exhausted side is a prefix of the other.
      return rhsBytes.empty() <=> lhsBytes.empty();
    }
    const auto n = std::min(lhsBytes.size(), rhsBytes.size());
    if (int c = std::memcmp(lhsBytes.data(), rhsBytes.data(), n); c != 0) {
      return c <=> 0;
    }
    lhs.skip(n);
    rhs.skip(n);
  }
}

template <typename CompareAt>
std::partial_ordering lexicographicalCompare(
    std::size_t lhsSize, std::size_t rhsSize, CompareAt compareAt) {
  for (std::size_t i = 0; i < std::min(lhsSize, rhsSize); ++i) {
    if (auto c = compareAt(i); c != kEquivalent) {
      return c;
    }
  }
  return lhsSize <=> rhsSize;
}

// std::stable_sort, unlike std::sort, stays in bounds when NaN makes the
// ordering inconsistent.
template <typename T, typename Less>
std::vector<T> sortedCopy(const std::vector<T>& items, Less less) {
  std::vector<std::size_t> order(items.size());
  std::iota(order.begin(), order.end(), std::size_t{0});
  std::stable_sort(order.begin(), order.end(), [&](auto lhs, auto rhs) {
    return less(items[lhs], items[rhs]);
  });
  std::vector<T> sorted;
  sorted.reserve(items.size());
  for (auto i : order) {
    sorted.push_back(items[i]);
  }
  return sorted;
}

std::partial_ordering compareLists(const List& lhs, const List& rhs) {
  return lexicographicalCompare(lhs.size(), rhs.size(), [&](std::size_t i) {
    return stableCompare(lhs[i], rhs[i]);
  });
}

std::partial_ordering compareSets(const Set& lhs, const Set& rhs) {
  const auto lhsElements = sortedElements(lhs);
  const auto rhsElements = sortedElements(rhs);
  return lexicographicalCompare(
      lhsElements.size(), rhsElements.size(), [&](std::size_t i) {
        return stableCompare(lhsElements[i], rhsElements[i]);
      });
}

std::partial_ordering compareMaps(const Map& lhs, const Map& rhs) {
  const auto lhsEntries = sortedEntries(lhs);
  const auto rhsEntries = sortedEntries(rhs);
  return lexicographicalCompare(
      lhsEntries.size(), rhsEntries.size(), [&](std::size_t i) {
        const auto& [lhsKey, lhsValue] = lhsEntries[i];
        const auto& [rhsKey, rhsValue] = rhsEntries[i];
        if (auto c = stableCompare(lhsKey, rhsKey); c != kEquivalent) {
          return c;
        }
        return stableCompare(lhsValue, rhsValue);
      });
}

std::partial_ordering compareStructs(const Struct& lhs, const Struct& rhs) {
  const auto& structNode = lhs.type().asStructUnchecked();
  for (auto handle : structNode.fieldHandlesByIdAscending()) {
    auto lhsField = lhs.getField(handle);
    auto rhsField = rhs.getField(handle);
    if (!lhsField || !rhsField) {
      if (lhsField || rhsField) {
        return lhsField ? kGreater : kLess;
      }
      continue;
    }
    if (auto c = stableCompare(*lhsField, *rhsField); c != kEquivalent) {
      return c;
    }
  }
  return kEquivalent;
}

std::partial_ordering compareUnions(const Union& lhs, const Union& rhs) {
  if (lhs.isEmpty() || rhs.isEmpty()) {
    return rhs.isEmpty() <=> lhs.isEmpty();
  }
  const auto& unionNode = lhs.type().asUnionUnchecked();
  const auto lhsId = unionNode.at(lhs.activeField()).identity().id();
  const auto rhsId = unionNode.at(rhs.activeField()).identity().id();
  if (lhsId != rhsId) {
    // Fields compare in field id order, and all fields before the active one
    // are unset, so the union with the larger active field id orders first.
    return lhsId > rhsId ? kLess : kGreater;
  }
  return stableCompare(
      lhs.getField(lhs.activeField()), rhs.getField(rhs.activeField()));
}

} // namespace

std::partial_ordering stableCompare(
    const DynamicConstRef& lhs, const DynamicConstRef& rhs) {
  using Kind = type_system::TypeRef::Kind;
  return lhs.type().matchKind(
      [&]<Kind k>(
          type_system::TypeRef::KindConstant<k>) -> std::partial_ordering {
        const auto& l = lhs.as<k>();
        const auto& r = rhs.as<k>();
        using T = std::remove_cvref_t<decltype(l)>;
        if constexpr (std::is_arithmetic_v<T>) {
          // Including enums, which are stored as i32
          return std::weak_order(l, r);
        } else if constexpr (std::is_same_v<T, String>) {
          return l.view() <=> r.view();
        } else if constexpr (std::is_same_v<T, Binary>) {
          // A default binary has no buffer for a cursor to read.
          if (l.empty() || r.empty()) {
            return !l.empty() <=> !r.empty();
          }
          return compareBytes(l.cursor(), r.cursor());
        } else if constexpr (std::is_same_v<T, Any>) {
          return op::detail::StableComparePolicy<
              type::struct_t<type::AnyStruct>>{}(l.toThrift(), r.toThrift());
        } else if constexpr (std::is_same_v<T, List>) {
          return compareLists(l, r);
        } else if constexpr (std::is_same_v<T, Set>) {
          return compareSets(l, r);
        } else if constexpr (std::is_same_v<T, Map>) {
          return compareMaps(l, r);
        } else if constexpr (std::is_same_v<T, Struct>) {
          return compareStructs(l, r);
        } else {
          static_assert(std::is_same_v<T, Union>);
          return compareUnions(l, r);
        }
      });
}

std::vector<DynamicConstRef> sortedElements(const Set& set) {
  return sortedCopy(
      std::vector<DynamicConstRef>(set.begin(), set.end()),
      [](const DynamicConstRef& lhs, const DynamicConstRef& rhs) {
        return stableCompare(lhs, rhs) == kLess;
      });
}

std::vector<std::pair<DynamicConstRef, DynamicConstRef>> sortedEntries(
    const Map& map) {
  using Entry = std::pair<DynamicConstRef, DynamicConstRef>;
  return sortedCopy(
      std::vector<Entry>(map.begin(), map.end()),
      [](const Entry& lhs, const Entry& rhs) {
        return stableCompare(lhs.first, rhs.first) == kLess;
      });
}

} // namespace apache::thrift::dynamic::detail
