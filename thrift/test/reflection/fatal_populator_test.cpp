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

#include <iomanip>
#include <folly/Memory.h>

#include <thrift/lib/cpp2/reflection/populator.h>
#include <thrift/test/reflection/fatal_serialization_common.h>
#include <thrift/test/reflection/gen-cpp2/simple_reflection_types.h>
#include <thrift/test/reflection/gen-cpp2/simple_reflection_types_custom_protocol.h>

namespace test_cpp2::simple_cpp_reflection {

using namespace apache::thrift;
using namespace apache::thrift::test;
using apache::thrift::populator::populator_opts;

TYPED_TEST_CASE(MultiProtocolTest, protocol_type_pairs);

TYPED_TEST(MultiProtocolTest, test_structs_populate) {
  populator_opts opts;
  std::mt19937 rng;

  for (int i = 0; i < 100; i++) {
    struct7 a, b;
    populator::populate(a, opts, rng);
    a.write(&this->writer);
    this->prep_read();
    this->debug_buffer();
    b.read(&this->reader);

    ASSERT_EQ(*a.field1(), *b.field1());
    ASSERT_EQ(*a.field2(), *b.field2());
    ASSERT_EQ(*a.field3(), *b.field3());
    ASSERT_EQ(*a.field4(), *b.field4());
    ASSERT_EQ(*a.field5(), *b.field5());
    ASSERT_EQ(*a.field6(), *b.field6());
    ASSERT_EQ(*a.field7(), *b.field7());
    ASSERT_EQ(*a.field8(), *b.field8());
    ASSERT_EQ(*a.field9(), *b.field9());
    ASSERT_EQ(*(a.field10()), *(b.field10()));

    auto abuf = a.field11()->coalesce();
    auto bbuf = b.field11()->coalesce();

    ASSERT_EQ(abuf.size(), bbuf.size());
    ASSERT_EQ(abuf, bbuf);

    ASSERT_EQ(*a.field12(), *b.field12());
    if (a.field13()) {
      ASSERT_EQ(*(a.field13()), *(b.field13()));
    } else {
      ASSERT_EQ(nullptr, b.field13());
    }
    ASSERT_EQ(*(a.field14()), *(b.field14()));
    ASSERT_EQ(*(a.field15()), *(b.field15()));
    ASSERT_EQ(*a.field16(), *b.field16());
    ASSERT_EQ(*a.field17(), *b.field17());
  }
}

TYPED_TEST(MultiProtocolTest, test_unions_populate) {
  std::mt19937 rng;
  populator_opts opts;

  for (int i = 0; i < 100; i++) {
    union1 a, b;
    populator::populate(a, opts, rng);
    a.write(&this->writer);
    this->prep_read();
    this->debug_buffer();
    b.read(&this->reader);

    ASSERT_EQ(a, b);
  }
}

TYPED_TEST(MultiProtocolTest, test_populating_optional_fields_p0) {
  std::mt19937 rng;
  populator_opts opts;
  opts.optional_field_prob = 0;

  uint32_t set_count = 0;
  for (int i = 0; i < 100; i++) {
    nested1 t;
    populator::populate(t, opts, rng);
    if (t.nfield00().has_value()) {
      set_count++;
    }
  }
  ASSERT_EQ(0, set_count);
}

TYPED_TEST(MultiProtocolTest, test_populating_optional_fields_p30) {
  std::mt19937 rng;
  populator_opts opts;
  opts.optional_field_prob = 0.3;

  uint32_t set_count = 0;
  for (int i = 0; i < 1000; i++) {
    nested1 t;
    populator::populate(t, opts, rng);
    if (t.nfield00().has_value()) {
      set_count++;
    }
  }
  // For 1000 samples, p=30%, confidence level 0.9999, Wilson confidence
  // interval is [0.246945 , 0.359020]
  ASSERT_NEAR(300, set_count, 60);
}

TYPED_TEST(MultiProtocolTest, test_populating_optional_fields_p100) {
  std::mt19937 rng;
  populator_opts opts;
  opts.optional_field_prob = 1.0;

  uint32_t set_count = 0;
  for (int i = 0; i < 100; i++) {
    nested1 t;
    populator::populate(t, opts, rng);
    if (t.nfield00().has_value()) {
      set_count++;
    }
  }
  ASSERT_EQ(100, set_count);
}

TYPED_TEST(MultiProtocolTest, test_populating_recursive_type) {
  std::mt19937 rng;
  populator_opts opts;
  opts.optional_field_prob = 1.0;
  opts.recursion_limit = 5;
  recursive1 t;
  uint32_t count = 0;
  for (populator::populate(t, opts, rng); t.next().has_value();
       t = t.next().value()) {
    count++;
  }
  // There should be `opts.recursion_limit` number of fully populated
  // `recursive1`s and one additonal default-initialized `recursive1` at the end
  ASSERT_EQ(opts.recursion_limit + 1, count);
}

// Populate two otherwise-identical structs (one plain, one with
// TemplatedTestFieldAdapter on every field) using the same seed.  The inner
// thrift values produced by the field-adapted struct must match the values
// produced by the plain struct, ensuring the field-adapter populator path
// consumes the RNG identically to the non-adapted path.
TYPED_TEST(MultiProtocolTest, test_field_adapter_populator) {
  populator_opts opts;
  opts.random_container_size = false;
  opts.list_len = 4;
  opts.map_len = 4;
  opts.str_len = 8;

  for (int i = 0; i < 100; i++) {
    std::mt19937 rng_plain(i);
    std::mt19937 rng_adapted(i);

    populator_fields_no_adapter plain;
    populator_fields_with_adapter adapted;
    populator::populate(plain, opts, rng_plain);
    populator::populate(adapted, opts, rng_adapted);

    // Scalar: i64
    ASSERT_EQ(*plain.field1(), adapted.field1()->value);
    // String
    ASSERT_EQ(*plain.field2(), adapted.field2()->value);
    // Container: list<i32>
    ASSERT_EQ(*plain.field3(), adapted.field3()->value);
    // Container: map<string, i64>
    ASSERT_EQ(*plain.field4(), adapted.field4()->value);

    // Verify TemplatedTestFieldAdapter sets the correct fieldId.
    ASSERT_EQ(adapted.field1()->fieldId, 1);
    ASSERT_EQ(adapted.field2()->fieldId, 2);
    ASSERT_EQ(adapted.field3()->fieldId, 3);
    ASSERT_EQ(adapted.field4()->fieldId, 4);
  }
}
} // namespace test_cpp2::simple_cpp_reflection
