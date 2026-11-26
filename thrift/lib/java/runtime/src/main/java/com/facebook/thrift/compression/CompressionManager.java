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

import com.facebook.thrift.compression.lz4.Lz4Compressor;
import com.facebook.thrift.compression.zlib.ZlibCompressor;
import com.facebook.thrift.compression.zstd.ZstdCompressor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.thrift.CodecConfig;
import org.apache.thrift.CompressionAlgorithm;
import org.apache.thrift.CompressionConfig;
import org.apache.thrift.Lz4CompressionLevelPreset;
import org.apache.thrift.TTransform;
import org.apache.thrift.ZlibCompressionLevelPreset;
import org.apache.thrift.ZstdCompressionLevelPreset;

/**
 * Central registry that maps THeader transform IDs and CompressionAlgorithm values to {@link
 * ThriftCompressor} instances. Mirrors C++ CompressionManager + CompressionAlgorithmSelector.
 *
 * <p>TTransform IDs are the wire-format values from THeader (defined in RpcMetadata.thrift).
 * CompressionAlgorithm values are the Rocket metadata values (different numbering).
 */
public final class CompressionManager {

  private CompressionManager() {}

  /**
   * Maps a TTransform wire ID to a CompressionAlgorithm. Mirrors C++
   * CompressionAlgorithmSelector::fromTTransform().
   */
  public static CompressionAlgorithm fromTTransform(int transformId) {
    TTransform transform = TTransform.fromInteger(transformId);
    if (transform == null) {
      throw new IllegalArgumentException("Unknown TTransform ID: " + transformId);
    }
    switch (transform) {
      case NONE:
        return CompressionAlgorithm.NONE;
      case ZLIB:
        return CompressionAlgorithm.ZLIB;
      case ZSTD:
        return CompressionAlgorithm.ZSTD;
      case LZ4:
        return CompressionAlgorithm.LZ4;
      case CUSTOM:
        return CompressionAlgorithm.CUSTOM;
      case ZLIB_LESS:
        return CompressionAlgorithm.ZLIB_LESS;
      case ZSTD_LESS:
        return CompressionAlgorithm.ZSTD_LESS;
      case LZ4_LESS:
        return CompressionAlgorithm.LZ4_LESS;
      case ZLIB_MORE:
        return CompressionAlgorithm.ZLIB_MORE;
      case ZSTD_MORE:
        return CompressionAlgorithm.ZSTD_MORE;
      case LZ4_MORE:
        return CompressionAlgorithm.LZ4_MORE;
      default:
        throw new IllegalArgumentException("Unknown TTransform: " + transform);
    }
  }

  /**
   * Returns the compressor for a given CompressionAlgorithm. For decompression, the level variant
   * (LESS/MORE) doesn't matter — all ZSTD variants use the same decompressor, etc.
   */
  public static ThriftCompressor getCompressor(CompressionAlgorithm algorithm) {
    switch (algorithm) {
      case ZLIB:
      case ZLIB_LESS:
      case ZLIB_MORE:
        return ZlibCompressor.INSTANCE;
      case ZSTD:
      case ZSTD_LESS:
      case ZSTD_MORE:
        return ZstdCompressor.INSTANCE;
      case LZ4:
      case LZ4_LESS:
      case LZ4_MORE:
        return Lz4Compressor.INSTANCE;
      case NONE:
        throw new IllegalArgumentException("Cannot get compressor for NONE");
      case CUSTOM:
        throw new UnsupportedOperationException("CUSTOM compression is not supported");
      default:
        throw new IllegalArgumentException("Unknown compression algorithm: " + algorithm);
    }
  }

  /**
   * Undoes THeader transforms in reverse order, matching C++ THeader::untransform(). Takes
   * ownership of the input buffer.
   *
   * @param transforms the TTransform wire IDs from the THeader frame, outermost first
   * @param allocator the allocator for the output buffers
   * @param data the transformed data; ownership is transferred
   * @return the untransformed data; caller owns and must release
   */
  public static ByteBuf untransform(
      List<Integer> transforms, ByteBufAllocator allocator, ByteBuf data) {
    for (int i = transforms.size() - 1; i >= 0; i--) {
      data = decompressFromTransform(transforms.get(i), allocator, data);
    }

    return data;
  }

  /**
   * Decompresses data using the algorithm indicated by the THeader transform ID. Takes ownership of
   * the input buffer.
   *
   * @param transformId the TTransform wire ID from the THeader frame
   * @param allocator the allocator for the output buffer
   * @param data the compressed data; ownership is transferred
   * @return the decompressed data; caller owns and must release
   */
  public static ByteBuf decompressFromTransform(
      int transformId, ByteBufAllocator allocator, ByteBuf data) {
    CompressionAlgorithm algorithm;
    try {
      algorithm = fromTTransform(transformId);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(data);
      throw t;
    }

    return decompress(algorithm, allocator, data);
  }

  /**
   * Decompresses data using the given CompressionAlgorithm. Takes ownership of the input buffer.
   *
   * @param algorithm the compression algorithm
   * @param allocator the allocator for the output buffer
   * @param data the compressed data; ownership is transferred
   * @return the decompressed data; caller owns and must release
   */
  public static ByteBuf decompress(
      CompressionAlgorithm algorithm, ByteBufAllocator allocator, ByteBuf data) {
    if (algorithm == null || algorithm == CompressionAlgorithm.NONE) {
      return data;
    }
    return getCompressorOrRelease(algorithm, data).decompress(allocator, data);
  }

  /**
   * Compresses data using the given CompressionAlgorithm. Takes ownership of the input buffer.
   *
   * @param algorithm the compression algorithm
   * @param allocator the allocator for the output buffer
   * @param data the data to compress; ownership is transferred
   * @return the compressed data; caller owns and must release
   */
  public static ByteBuf compress(
      CompressionAlgorithm algorithm, ByteBufAllocator allocator, ByteBuf data) {
    if (algorithm == null || algorithm == CompressionAlgorithm.NONE) {
      return data;
    }
    return getCompressorOrRelease(algorithm, data).compress(allocator, data);
  }

  /** Releases data when no compressor exists, because the caller transferred its ownership. */
  private static ThriftCompressor getCompressorOrRelease(
      CompressionAlgorithm algorithm, ByteBuf data) {
    try {
      return getCompressor(algorithm);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(data);
      throw t;
    }
  }

  /**
   * Compresses data with each THeader transform in order, as C++ {@code THeader::transform} does.
   * Takes ownership of the input buffer.
   *
   * @param transformIds the TTransform wire IDs, in the order they are applied
   * @param allocator the allocator for the output buffer
   * @param data the data to compress; ownership is transferred
   * @return the compressed data; caller owns and must release
   */
  public static ByteBuf compressForTransforms(
      List<Integer> transformIds, ByteBufAllocator allocator, ByteBuf data) {
    for (int transformId : transformIds) {
      data = compress(fromTTransform(transformId), allocator, data);
    }
    return data;
  }

  /**
   * Returns the algorithm of a codec config. Mirrors C++
   * CompressionAlgorithmSelector::fromCodecConfig().
   */
  public static CompressionAlgorithm fromCodecConfig(CodecConfig codecConfig) {
    if (codecConfig.isSetZlibConfig()) {
      return fromZlibLevel(codecConfig.getZlibConfig().getLevelPreset());
    }

    if (codecConfig.isSetZstdConfig()) {
      return fromZstdLevel(codecConfig.getZstdConfig().getLevelPreset());
    }

    if (codecConfig.isSetLz4Config()) {
      return fromLz4Level(codecConfig.getLz4Config().getLevelPreset());
    }

    if (codecConfig.isSetCustomConfig()) {
      return CompressionAlgorithm.CUSTOM;
    }

    return CompressionAlgorithm.NONE;
  }

  /**
   * Returns the algorithm to compress a payload with: the codec of the config when the payload is
   * larger than its size limit (default 0), else NONE. Mirrors C++
   * CompressionManager::setCompressionCodec(). CUSTOM gives NONE, because the Java runtime has no
   * custom compressor.
   */
  public static CompressionAlgorithm algorithmFor(
      @Nullable CompressionConfig compressionConfig, long payloadSize) {
    if (compressionConfig == null || compressionConfig.getCodecConfig() == null) {
      return CompressionAlgorithm.NONE;
    }

    Long sizeLimit = compressionConfig.getCompressionSizeLimit();
    if (payloadSize <= (sizeLimit == null ? 0 : sizeLimit)) {
      return CompressionAlgorithm.NONE;
    }

    CompressionAlgorithm algorithm = fromCodecConfig(compressionConfig.getCodecConfig());
    return algorithm == CompressionAlgorithm.CUSTOM ? CompressionAlgorithm.NONE : algorithm;
  }

  /**
   * Returns the THeader transform IDs to compress a request payload with, as C++
   * HeaderClientChannel::setRequestHeaderOptions() does. NONE gives no transform.
   */
  public static List<Integer> headerTransformsFor(
      @Nullable CompressionConfig compressionConfig, long payloadSize) {
    CompressionAlgorithm algorithm = algorithmFor(compressionConfig, payloadSize);
    if (algorithm == CompressionAlgorithm.NONE) {
      return Collections.emptyList();
    }

    return Collections.singletonList(toTTransform(algorithm).getValue());
  }

  /** Mirrors C++ CompressionAlgorithmSelector::toTTransform(). */
  public static TTransform toTTransform(CompressionAlgorithm algorithm) {
    switch (algorithm) {
      case NONE:
        return TTransform.NONE;
      case ZLIB:
        return TTransform.ZLIB;
      case ZSTD:
        return TTransform.ZSTD;
      case LZ4:
        return TTransform.LZ4;
      case CUSTOM:
        return TTransform.CUSTOM;
      case ZLIB_LESS:
        return TTransform.ZLIB_LESS;
      case ZSTD_LESS:
        return TTransform.ZSTD_LESS;
      case LZ4_LESS:
        return TTransform.LZ4_LESS;
      case ZLIB_MORE:
        return TTransform.ZLIB_MORE;
      case ZSTD_MORE:
        return TTransform.ZSTD_MORE;
      case LZ4_MORE:
        return TTransform.LZ4_MORE;
      default:
        throw new IllegalArgumentException("Unknown compression algorithm: " + algorithm);
    }
  }

  private static CompressionAlgorithm fromZlibLevel(@Nullable ZlibCompressionLevelPreset level) {
    if (level == ZlibCompressionLevelPreset.LESS) {
      return CompressionAlgorithm.ZLIB_LESS;
    }

    return level == ZlibCompressionLevelPreset.MORE
        ? CompressionAlgorithm.ZLIB_MORE
        : CompressionAlgorithm.ZLIB;
  }

  private static CompressionAlgorithm fromZstdLevel(@Nullable ZstdCompressionLevelPreset level) {
    if (level == ZstdCompressionLevelPreset.LESS) {
      return CompressionAlgorithm.ZSTD_LESS;
    }

    return level == ZstdCompressionLevelPreset.MORE
        ? CompressionAlgorithm.ZSTD_MORE
        : CompressionAlgorithm.ZSTD;
  }

  private static CompressionAlgorithm fromLz4Level(@Nullable Lz4CompressionLevelPreset level) {
    if (level == Lz4CompressionLevelPreset.LESS) {
      return CompressionAlgorithm.LZ4_LESS;
    }

    return level == Lz4CompressionLevelPreset.MORE
        ? CompressionAlgorithm.LZ4_MORE
        : CompressionAlgorithm.LZ4;
  }
}
