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

include "thrift/annotation/cpp.thrift"

@cpp.EnableIOBufChainSupport
package "apache.org/thrift/test/iobuf_chain_rpc"

cpp_include "thrift/lib/cpp2/IOBufChain.h"

union Choice {
  1: i64 number;
  2: string text;
}

struct Payload {
  1: i64 sequence;
  2: string text;
  3: binary ordinaryBinary;
  @cpp.Type{name = "::apache::thrift::IOBufChain"}
  4: binary chainBinary;
  5: Choice choice;
  @cpp.Type{name = "std::unique_ptr<folly::IOBuf>"}
  6: binary iobufBinary;
}

service ChainService {
  Payload echo(1: Payload payload);
}
