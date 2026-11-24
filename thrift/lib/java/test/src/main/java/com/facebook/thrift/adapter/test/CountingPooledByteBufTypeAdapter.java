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

import com.facebook.thrift.adapter.TypeAdapter;
import com.facebook.thrift.util.ThriftSizes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class CountingPooledByteBufTypeAdapter implements TypeAdapter<ByteBuf, ByteBuf> {
  /** Counts `toThrift` calls so sizing tests can prove elements are not converted. */
  public static long toThriftCalls;

  /** Counts hook calls so sizing tests can prove the hook sizes elements instead. */
  public static long hookCalls;

  @Override
  public ByteBuf fromThrift(ByteBuf value) {
    return value;
  }

  @Override
  public ByteBuf toThrift(ByteBuf value) {
    toThriftCalls++;

    int length = value.readableBytes();
    ByteBuf copy = PooledByteBufAllocator.DEFAULT.directBuffer(length, length);
    copy.writeBytes(value, value.readerIndex(), length);

    return copy;
  }

  @Override
  public long serializedSize(ByteBuf value) {
    hookCalls++;

    return value == null ? 0 : ThriftSizes.binary(value.readableBytes());
  }
}
