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

// @noautodeps

#pragma once

#include <thrift/lib/cpp2/Flags.h>

THRIFT_FLAG_DECLARE_bool(thrift_client_compress_request_on_cpu);
THRIFT_FLAG_DECLARE_bool(
    thrift_client_decode_custom_response_when_skipping_decompression);

namespace apache::thrift {
inline bool clientCompressRequestOnCpu() {
  return THRIFT_FLAG(thrift_client_compress_request_on_cpu);
}
} // namespace apache::thrift
