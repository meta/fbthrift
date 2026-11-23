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

#include <new>

#include <thrift/lib/cpp2/gen/module_types_cpp.h>
#include <thrift/lib/cpp2/gen/tcc_struct_traits.h>

namespace apache::thrift::detail {

template <typename T, typename Enable>
void TccStructTraits<T, Enable>::translateFieldName(
    std::string_view _fname,
    int16_t& fid,
    apache::thrift::protocol::TType& _ftype) {
  using data = apache::thrift::TStructDataStorage<T>;
  static constexpr st::translate_field_name_table metadata{
      data::fields_size,
      data::fields_names.data(),
      data::fields_ids.data(),
      data::fields_types.data()};
  // Linear scan beats hashing for small structs (threshold 12 from
  // benchmarking).
  if constexpr (data::fields_size <= 12) {
    st::translate_field_name_or_id(_fname, fid, _ftype, metadata);
  } else {
    static st::translate_field_name_cache cache{metadata};
    if (folly::test_once(cache.once)) [[likely]] {
      const auto& table = *std::launder(
          reinterpret_cast<const st::translate_field_name_hash_table*>(
              &cache.storage));
      st::translate_field_name_or_id(_fname, fid, _ftype, table);
    } else {
      st::translate_field_name_or_id(_fname, fid, _ftype, cache);
    }
  }
}

} // namespace apache::thrift::detail
