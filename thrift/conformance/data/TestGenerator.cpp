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

#include <thrift/conformance/data/TestGenerator.h>

#include <string_view>

#include <fmt/core.h>

#include <thrift/conformance/data/TestUtil.h>
#include <thrift/conformance/data/ValueGenerator.h>
#include <thrift/lib/cpp2/type/Tag.h>
#include <thrift/test/testset/Testset.h>
#include <thrift/test/testset/gen-cpp2/Terse_types.h>

namespace mp11 = boost::mp11;
using apache::thrift::protocol::asValueStruct;
using apache::thrift::test::testset::FieldModifier;

namespace apache::thrift::conformance::data {

namespace {

template <typename ElementTag>
Test createRoundTripTest(
    const AnyRegistry& registry, const Protocol& protocol) {
  using namespace apache::thrift::test::testset::detail;

  const std::string& typeName = getTestTypeName<ElementTag>();
  Test test;
  test.name() = protocol.name();
  test.tags()->emplace("idl/");
  for (const auto& value : ValueGenerator<ElementTag>::getInterestingValues()) {
    RoundTripTestCase roundTrip;

    // Test case #1: Use ValueStruct
    roundTrip.request()->value() =
        registry.store(asValueStruct<ElementTag>(value.value), protocol);

    auto& testCase = test.testCases()->emplace_back();
    testCase.name() = fmt::format("{}/{}", typeName, value.name);
    testCase.test()->roundTrip() = roundTrip;

    auto addTestCase = [&](auto data, auto msg) {
      roundTrip.request()->value() = registry.store(data, protocol);
      auto& currentTestCase = test.testCases()->emplace_back();
      currentTestCase.name() = msg;
      currentTestCase.test()->roundTrip() = roundTrip;
    };

    auto addStruct = [&](auto modSet, auto msg) {
      using ModSet = decltype(modSet);
      typename struct_ByFieldType<ElementTag, ModSet>::type data;
      data.field_1() = type::native_type<ElementTag>{value.value};
      addTestCase(
          data, fmt::format("testset.{}{}/{}", msg, typeName, value.name));
    };

    // Test case #2: Unqualified field
    addStruct(mod_set<>{}, "");

    typename union_ByFieldType<ElementTag, mod_set<>>::type unionData;
    unionData.field_1_ref() = type::native_type<ElementTag>{value.value};
    addTestCase(
        unionData, fmt::format("testset.union.{}/{}", typeName, value.name));

    // Test case #3: Optional field
    addStruct(mod_set<FieldModifier::Optional>{}, "Optional.");

    // Test case #4: Terse field
    addStruct(mod_set<FieldModifier::Terse>{}, "Terse.");
  }

  // Test case: Optional field with empty value (generated once, not per value)
  {
    RoundTripTestCase roundTrip;
    roundTrip.request()->value() = registry.store(
        typename struct_ByFieldType<
            ElementTag,
            mod_set<FieldModifier::Optional>>::type{},
        protocol);
    auto& emptyOptTestCase = test.testCases()->emplace_back();
    emptyOptTestCase.name() =
        fmt::format("testset.Optional.{}/empty_optional", typeName);
    emptyOptTestCase.test()->roundTrip() = roundTrip;
  }

  return test;
}

template <typename T, typename Set>
T make(Set set) {
  T value;
  set(value);
  return value;
}

template <
    typename ElementTag,
    typename Struct,
    typename AddTestCase,
    typename Set>
void addTerseValueTestCases(
    AddTestCase& addTestCase, std::string_view kind, Set set) {
  for (const auto& value : ValueGenerator<ElementTag>::getInterestingValues()) {
    addTestCase(
        make<Struct>([&](auto& data) { set(data, value.value); }),
        fmt::format(
            "{}.{}/{}", kind, getTestTypeName<ElementTag>(), value.name));
  }
}

Test createTerseRoundTripTest(
    const AnyRegistry& registry, const Protocol& protocol) {
  using namespace apache::thrift::test::testset;

  Test test;
  test.name() = protocol.name();
  test.tags()->emplace("idl/");
  auto addTestCase = [&](const auto& data, std::string_view name) {
    RoundTripTestCase roundTrip;
    roundTrip.request()->value() = registry.store(data, protocol);
    auto& testCase = test.testCases()->emplace_back();
    testCase.name() = fmt::format("testset.Terse.{}", name);
    testCase.test()->roundTrip() = std::move(roundTrip);
  };
  auto setField = [](auto& data, const auto& value) { data.field() = value; };

  addTestCase(TerseEnumStruct{}, "enum/zero");
  addTestCase(
      make<TerseEnumStruct>([](auto& data) { data.field() = TerseEnum::one; }),
      "enum/one");

  addTestCase(TerseUnionStruct{}, "union/empty");
  addTestCase(
      make<TerseUnionStruct>([](auto& data) { data.field()->set_field_1(); }),
      "union/default_member");

  addTerseValueTestCases<type::i32_t, TerseNewTypeI32Struct>(
      addTestCase, "NewType", setField);
  addTerseValueTestCases<type::string_t, TerseNewTypeStringStruct>(
      addTestCase, "NewType", setField);
  addTerseValueTestCases<type::list<type::i32_t>, TerseNewTypeListI32Struct>(
      addTestCase, "NewType", setField);

  addTestCase(TerseEmptiableStruct{}, "struct/empty");
  addTestCase(
      make<TerseEmptiableStruct>(
          [](auto& data) { data.field()->optional_field() = 0; }),
      "struct/optional_zero");
  addTestCase(
      make<TerseEmptiableStruct>(
          [](auto& data) { data.field()->terse_field() = "a"; }),
      "struct/terse_lower");
  addTestCase(TerseNonEmptiableStruct{}, "struct/unqualified_zero");

  addTestCase(TerseEmptiableExceptionStruct{}, "exception/empty");
  addTestCase(
      make<TerseEmptiableExceptionStruct>(
          [](auto& data) { data.field()->message() = ""; }),
      "exception/optional_empty");

  addTerseValueTestCases<type::string_t, TerseAdaptedFieldStruct>(
      addTestCase, "RustAdapter", setField);
  addTerseValueTestCases<type::string_t, TerseAdaptedTypedefStruct>(
      addTestCase, "RustAdapter.typedef", setField);
  addTerseValueTestCases<type::string_t, TerseNestedAdaptedFieldStruct>(
      addTestCase,
      "struct.Terse.RustAdapter",
      [](auto& data, const auto& value) { data.field()->field() = value; });

  return test;
}

void addRoundTripToSuite(
    const AnyRegistry& registry, const Protocol& protocol, TestSuite& suite) {
  mp11::mp_for_each<detail::PrimaryTypeTags>([&](auto element_tag) {
    using ElementTag = decltype(element_tag);
    suite.tests()->emplace_back(
        createRoundTripTest<ElementTag>(registry, protocol));
    if constexpr (!std::is_same_v<ElementTag, type::bool_t>) {
      suite.tests()->emplace_back(
          createRoundTripTest<type::list<ElementTag>>(registry, protocol));
    }
    mp11::mp_for_each<detail::KeyTypeTags>([&](auto key_tag) {
      using KeyTag = decltype(key_tag);
      suite.tests()->emplace_back(
          createRoundTripTest<type::map<KeyTag, ElementTag>>(
              registry, protocol));
    });
  });
  mp11::mp_for_each<detail::KeyTypeTags>([&](auto key_tag) {
    using KeyTag = decltype(key_tag);
    suite.tests()->emplace_back(
        createRoundTripTest<type::set<KeyTag>>(registry, protocol));
  });
  suite.tests()->emplace_back(createTerseRoundTripTest(registry, protocol));
}

} // namespace

TestSuite createRoundTripSuite(
    const std::set<Protocol>& protocols, const AnyRegistry& registry) {
  TestSuite suite;
  suite.name() = "RoundTripTest";
  for (const auto& protocol : protocols) {
    addRoundTripToSuite(registry, protocol, suite);
  }
  return suite;
}

} // namespace apache::thrift::conformance::data
