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

#include <thrift/lib/cpp2/dynamic/DynamicValue.h>

#include <compare>
#include <utility>
#include <vector>

namespace apache::thrift::dynamic::detail {

/**
 * Orders two values of the same type like op::detail::StableComparePolicy
 * orders static values: struct fields by ascending field id (an unset field
 * orders first), unions by active field (a larger field id orders first) and
 * then by value, lists lexicographically, and sets and maps lexicographically
 * by their sorted elements. Floating-point values order -NaN first and NaN
 * last, with -0.0 equivalent to 0.0.
 */
std::partial_ordering stableCompare(
    const DynamicConstRef& lhs, const DynamicConstRef& rhs);

// The elements of `set`, sorted by stableCompare.
std::vector<DynamicConstRef> sortedElements(const Set& set);

// The entries of `map`, sorted by stableCompare of their keys.
std::vector<std::pair<DynamicConstRef, DynamicConstRef>> sortedEntries(
    const Map& map);

} // namespace apache::thrift::dynamic::detail
