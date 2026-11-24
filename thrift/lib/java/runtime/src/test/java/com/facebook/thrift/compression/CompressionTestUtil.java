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

package com.facebook.thrift.compression;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

final class CompressionTestUtil {

  private CompressionTestUtil() {}

  static ByteBuf compositeInput(byte[] bytes) {
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    int half = bytes.length / 2;

    ByteBuf first = allocator.directBuffer(half).writeBytes(bytes, 0, half);
    ByteBuf second =
        allocator.directBuffer(bytes.length - half).writeBytes(bytes, half, bytes.length - half);

    return allocator.compositeDirectBuffer().addComponents(true, first, second);
  }

  static byte[] readAll(ByteBuf buf) {
    byte[] bytes = new byte[buf.readableBytes()];
    buf.getBytes(buf.readerIndex(), bytes);
    return bytes;
  }
}
