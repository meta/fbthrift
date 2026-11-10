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

namespace java test.fixtures.interactions
namespace java.swift test.fixtures.interactions
namespace py test.fixtures.interactions
namespace py3 test.fixtures.interactions

include "shared.thrift"
include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

@thrift.AllowLegacyMissingUris
package;

exception CustomException {
  1: string message;
}

interaction MyInteraction {
  i32 frobnicate() throws (1: CustomException ex);
  oneway void ping();
  stream<bool> truthify();
  stream<i32 throws (1: CustomException boom)> failstream();
  stream<i32> failstreaminit() throws (1: CustomException boom);
  i32 frobnicateWithArg(1: i32 arg) throws (1: CustomException ex);
  stream<i32> streamWithArg(1: string query) throws (1: CustomException boom);
  set<i32>, sink<string, binary> encode();
}

@cpp.ProcessInEbThreadUnsafe
interaction MyInteractionFast {
  i32 frobnicate();
  oneway void ping();
  stream<bool> truthify();
  set<i32>, sink<string, binary> encode();
}

@thrift.Serial
interaction SerialInteraction {
  void frobnicate();
}

service MyService {
  performs MyInteraction;
  performs MyInteractionFast;
  performs SerialInteraction;
  void foo();

  MyInteraction interact(1: i32 arg);
  MyInteractionFast, i32 interactFast();
  SerialInteraction, i32, stream<i32> serialize();
}

service Factories {
  void foo();

  MyInteraction interact(1: i32 arg);
  MyInteractionFast, i32 interactFast();
  SerialInteraction, i32, stream<i32> serialize();

  MyInteraction, stream<i32> interactWithStream();
  MyInteraction, i32, stream<i32> interactWithStreamAndInitialResponse();
  MyInteraction, sink<i32, i32> interactWithSink();
  MyInteraction, i32, sink<i32, i32> interactWithSinkAndInitialResponse();
  MyInteraction, sink<i32>, stream<i32> interactWithBidi();
  MyInteraction, i32, sink<i32>, stream<i32> interactWithBidiAndInitialResponse();
}

service Perform {
  performs MyInteraction;
  performs MyInteractionFast;
  performs SerialInteraction;
  void foo();
}

service InteractWithShared {
  shared.DoSomethingResult do_some_similar_things();
  performs MyInteraction;
  performs shared.SharedInteraction;
}

struct ShouldBeBoxed {
  1: string sessionId;
}

interaction BoxedInteraction {
  ShouldBeBoxed getABox();
}

service BoxService {
  BoxedInteraction, ShouldBeBoxed getABoxSession(1: ShouldBeBoxed req);
}
