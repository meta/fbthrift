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

include "thrift/annotation/thrift.thrift"

namespace cpp2 thrift.benchmark

@thrift.Experimental
package "apache.org/thrift/test/field_name_cache_bench"

struct Fields12 {
  1: optional i32 field01;
  2: optional i32 field02;
  3: optional i32 field03;
  4: optional i32 field04;
  5: optional i32 field05;
  6: optional i32 field06;
  7: optional i32 field07;
  8: optional i32 field08;
  9: optional i32 field09;
  10: optional i32 field10;
  11: optional i32 field11;
  12: optional i32 field12;
}

struct Fields13 {
  1: optional i32 field01;
  2: optional i32 field02;
  3: optional i32 field03;
  4: optional i32 field04;
  5: optional i32 field05;
  6: optional i32 field06;
  7: optional i32 field07;
  8: optional i32 field08;
  9: optional i32 field09;
  10: optional i32 field10;
  11: optional i32 field11;
  12: optional i32 field12;
  13: optional i32 field13;
}
