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

// The Rust generator recognizes the fb303 bases by scoped name, so this file
// has to be named `fb303_core.thrift` and declare `BaseService`. It is a
// stand-in for `fb303/thrift/fb303_core.thrift`, not a copy of it.

package "test.dev/fixtures/rust_fb303/fb303_core"

enum fb303_status {
  DEAD = 0,
  STARTING = 1,
  ALIVE = 2,
  STOPPING = 3,
  STOPPED = 4,
  WARNING = 5,
}

service BaseService {
  fb303_status getStatus();
  string getStatusDetails();
  string getName();
}
