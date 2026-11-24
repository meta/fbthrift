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

package com.facebook.thrift.adapter.test;

import com.facebook.thrift.adapter.common.StringTypeAdapter;
import com.facebook.thrift.util.ThriftSizes;

public class CountingHookStringAdapter implements StringTypeAdapter<String> {
  /** Counts hook calls so sizing tests can prove the hook replaces conversion. */
  public static long hookCalls;

  /** Counts conversions so sizing tests can prove the fallback converts. */
  public static long convertCalls;

  @Override
  public String fromThrift(String s) {
    return s;
  }

  @Override
  public String toThrift(String s) {
    convertCalls++;
    return s;
  }

  @Override
  public long serializedSize(String s) {
    hookCalls++;
    return s == null ? -1 : ThriftSizes.string(s);
  }
}
