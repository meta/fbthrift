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

#include <string_view>

#include <thrift/lib/cpp2/PluggableFunction.h>

namespace apache::thrift::protocol::detail {

// Called when `DynamicUnknownPatch` fails validation. Such patches are still
// accepted for backward compatibility; this lets us find out whether any caller
// actually produces them before we start rejecting them. No-op by default.
//
// `reason` is a short, stable identifier. It never contains patch content.
THRIFT_PLUGGABLE_FUNC_DECLARE(
    void, logUnclassifiedUnknownPatch, std::string_view reason);

} // namespace apache::thrift::protocol::detail
