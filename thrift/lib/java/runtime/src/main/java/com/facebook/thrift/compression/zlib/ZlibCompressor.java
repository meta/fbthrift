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

package com.facebook.thrift.compression.zlib;

import com.facebook.thrift.compression.ThriftCompressor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import reactor.core.Exceptions;

/**
 * ZLIB compression using the JDK's Inflater/Deflater (Java 8 base version). Matches C++
 * folly::compression::CodecType::ZLIB (RFC 1950 zlib wrapper format).
 *
 * <p>On Netty event loop threads ({@link FastThreadLocalThread}), Deflater and Inflater instances
 * are cached in {@link FastThreadLocal} to avoid native resource allocation per request.
 */
public final class ZlibCompressor implements ThriftCompressor {

  public static final ZlibCompressor INSTANCE = new ZlibCompressor();

  private static final int BUFFER_SIZE = 1024;

  private static final FastThreadLocal<Deflater> DEFLATER_CACHE = new FastThreadLocal<>();
  private static final FastThreadLocal<Inflater> INFLATER_CACHE = new FastThreadLocal<>();

  ZlibCompressor() {}

  @Override
  public ByteBuf compress(ByteBufAllocator allocator, ByteBuf data) {
    ByteBuf output = null;
    boolean cached = Thread.currentThread() instanceof FastThreadLocalThread;
    Deflater deflater = null;

    try {
      deflater = cached ? acquireDeflater() : new Deflater();
      byte[] input = ByteBufUtil.getBytes(data, data.readerIndex(), data.readableBytes(), false);
      deflater.setInput(input);
      deflater.finish();

      output = allocator.buffer();
      byte[] buffer = new byte[BUFFER_SIZE];
      while (!deflater.finished()) {
        int count = deflater.deflate(buffer);
        output.writeBytes(buffer, 0, count);
      }
      return output;
    } catch (Exception exception) {
      ReferenceCountUtil.safeRelease(output);
      throw exception;
    } finally {
      ReferenceCountUtil.safeRelease(data);

      if (deflater != null) {
        if (cached) {
          deflater.reset();
        } else {
          deflater.end();
        }
      }
    }
  }

  @Override
  public ByteBuf decompress(ByteBufAllocator allocator, ByteBuf data) {
    ByteBuf output = null;
    boolean cached = Thread.currentThread() instanceof FastThreadLocalThread;
    Inflater inflater = null;

    try {
      inflater = cached ? acquireInflater() : new Inflater();
      byte[] input = ByteBufUtil.getBytes(data, data.readerIndex(), data.readableBytes(), false);
      inflater.setInput(input);

      output = allocator.buffer();
      byte[] buffer = new byte[BUFFER_SIZE];
      while (!inflater.finished()) {
        int count = inflater.inflate(buffer);
        if (count == 0 && inflater.needsInput()) {
          break;
        }
        output.writeBytes(buffer, 0, count);
      }
      return output;
    } catch (DataFormatException exception) {
      ReferenceCountUtil.safeRelease(output);
      throw Exceptions.propagate(exception);
    } catch (Exception exception) {
      ReferenceCountUtil.safeRelease(output);
      throw exception;
    } finally {
      ReferenceCountUtil.safeRelease(data);

      if (inflater != null) {
        if (cached) {
          inflater.reset();
        } else {
          inflater.end();
        }
      }
    }
  }

  private static Deflater acquireDeflater() {
    Deflater deflater = DEFLATER_CACHE.get();
    if (deflater == null) {
      deflater = new Deflater();
      DEFLATER_CACHE.set(deflater);
    }
    return deflater;
  }

  private static Inflater acquireInflater() {
    Inflater inflater = INFLATER_CACHE.get();
    if (inflater == null) {
      inflater = new Inflater();
      INFLATER_CACHE.set(inflater);
    }
    return inflater;
  }
}
