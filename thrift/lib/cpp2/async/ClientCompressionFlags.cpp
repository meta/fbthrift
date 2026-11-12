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

#include <thrift/lib/cpp2/async/ClientCompressionFlags.h>

// Controls both request compression and response decompression offloading
// from IO thread to caller/consumer thread.
THRIFT_FLAG_DEFINE_bool(thrift_client_compress_request_on_cpu, false);
// Kill switch for forced CUSTOM decoding on compressed unary receive paths.
THRIFT_FLAG_DEFINE_bool(
    thrift_client_decode_custom_response_when_skipping_decompression, true);
