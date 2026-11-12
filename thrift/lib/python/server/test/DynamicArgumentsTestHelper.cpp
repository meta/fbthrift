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

#include <thrift/lib/python/server/test/DynamicArgumentsTestHelper.h>

#include <cstddef>

#include <fmt/core.h>

#include <thrift/lib/cpp2/Flags.h>

THRIFT_FLAG_DECLARE_bool(python_server_populate_service_schema);

namespace apache::thrift::python::test {

void setPopulateServiceSchemaForTest(bool enabled) {
  THRIFT_FLAG_SET_MOCK(python_server_populate_service_schema, enabled);
}

void unmockPopulateServiceSchemaForTest() {
  THRIFT_FLAG_UNMOCK(python_server_populate_service_schema);
}

#if FOLLY_HAS_COROUTINES
folly::coro::Task<std::optional<folly::Unit>>
DynamicArgumentsRecorder::onRequest(folly::Unit*, RequestInfo requestInfo) {
  records_.wlock()->push_back(render(requestInfo));
  co_return std::nullopt;
}
#endif

std::string DynamicArgumentsRecorder::render(const RequestInfo& requestInfo) {
  const auto* args = requestInfo.dynamicArguments;
  if (args == nullptr) {
    return fmt::format("{}:<no schema>", requestInfo.methodName);
  }
  std::string out(requestInfo.methodName);
  for (std::size_t i = 0; i < args->count(); ++i) {
    const auto name = args->name(i);
    const auto value = args->get(i);
    // Rendered per parameter of the test IDL, since reading a value means
    // naming its type.
    if (name == "point") {
      const auto& point = value.asStruct();
      out += fmt::format(
          " {}=({},{})",
          name,
          point.getField("x")->asI32(),
          point.getField("y")->asI32());
    } else if (name == "seq") {
      out += fmt::format(" {}={}", name, value.asI32());
    } else {
      out += fmt::format(" {}={}", name, value.asString().view());
    }
  }
  return out;
}

} // namespace apache::thrift::python::test
