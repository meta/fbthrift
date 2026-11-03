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

include "thrift/annotation/java.thrift"

package "test.dev/thrift/lib/java/test/utf8/pkg"

namespace java.swift com.facebook.thrift.test.utf8.pkg

struct TestStructPkg {
  1: string str_field;
}

struct TestStructReportPkg {
  @java.Utf8
  1: string str_field;
}

transient exception TestExceptionPkg {
  1: i32 err_code;
  2: string err_msg;
}

union TestUnionPkg {
  1: string field1;
  2: string field2;
}
