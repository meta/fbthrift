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

include "thrift/compiler/test/fixtures/rust-fb303/src/fb303.thrift"
include "thrift/compiler/test/fixtures/rust-fb303/src/fb303_core.thrift"

package "test.dev/fixtures/rust_fb303"

namespace java.swift test.fixtures.rust_fb303

// Even though the two services below extend fb303, codegen prevents them
// from inheriting its methods, so they can only be constructed with the
// 2-arg `make_<Service>_server`, which takes no parent factory.

// The deprecated inheritance is the shape under test.
// @lint-ignore THRIFTCHECKS facebook-service-deprecated
service ExtendsFacebookService extends fb303.FacebookService {
  void do_facebook();
}

service ExtendsBaseService extends fb303_core.BaseService {
  void do_base();
}

service MyRoot {
  void do_root();
}

// Has a parent, but not an fb303 one.
service ExtendsMyRoot extends MyRoot {
  void do_child();
}

// Transitively reaches fb303 through a non-fb303 direct parent.
service ExtendsExtendsBaseService extends ExtendsBaseService {
  void do_grandchild();
}

service NoParent {
  void do_standalone();
}
