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

// Stand-in for `common/fb303/if/fb303.thrift`; see the note in
// `fb303_core.thrift` for why the file and service names are fixed.

include "thrift/compiler/test/fixtures/rust/src/fb303/fb303_core.thrift"

package "test.dev/fixtures/rust_fb303/fb303"

service FacebookService extends fb303_core.BaseService {
  i64 aliveSince();
}
