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

#pragma once

#include <cstddef>

namespace apache::thrift::fast_thrift::frame::write {

/**
 * Configuration for FrameFragmentationHandler.
 *
 * Controls fragmentation behavior for HOL blocking mitigation.
 * See: FrameFragmentationHandler.md for design documentation.
 *
 * Usage with designated initializers:
 *   FragmentationHandlerConfig config{
 *       .maxFragmentSize = 32 * 1024,  // 32KB for low-latency
 *   };
 *
 * Or use defaults:
 *   FragmentationHandlerConfig config{};
 */
struct FragmentationHandlerConfig {
  /**
   * Largest data size of one fragment.
   *
   * A frame whose data does not fit in one fragment is split into fragments
   * with at most this many data bytes. No fragment goes above
   * `kMaxStreamQuantum` bytes, so a larger value acts as that limit. The first
   * fragment carries all metadata of the frame, so its metadata counts toward
   * that limit and its data can stop earlier. A frame goes through unsplit
   * when its data fits in that first fragment.
   *
   * Default: `0xffffff - 512` (~16 MiB minus 512 bytes) — matches legacy
   * `apache::thrift::rocket::kMaxFragmentedPayloadSize`
   * (`thrift/lib/cpp2/transport/rocket/framing/Frames.h`). The
   * 3-byte RSocket frame-length field caps at `0xffffff`; the 512-byte
   * margin leaves room for the per-frame header.
   *
   * At this default, fragmentation only kicks in for very large payloads
   * (multi-megabyte). Override to a smaller value (e.g., 64KB) to enable
   * SRPT-style HOL-blocking mitigation on smaller frames.
   */
  size_t maxFragmentSize{0xffffff - 512};

  /**
   * Maximum pending bytes before forcing a flush.
   *
   * If the total bytes queued across all streams exceeds this threshold,
   * the handler forces an immediate flush rather than waiting for the
   * EventBase LoopCallback.
   *
   * Default: 512KB - provides memory pressure limit while allowing batching.
   *
   * Tuning:
   *   - Memory-constrained: Decrease to 256KB
   *   - High-throughput bulk transfers: Increase to 1MB+
   */
  size_t maxPendingBytes{512 * 1024};

  /**
   * Maximum pending frames before forcing a flush.
   *
   * Backstop limit for the many-small-frames case where byte count
   * is low but frame count is high. Prevents unbounded queue growth.
   *
   * Default: 128 frames.
   */
  size_t maxPendingFrames{128};

  /**
   * Largest data size that goes downstream at once while other streams have
   * queued fragments.
   *
   * This value applies to a frame that needs no fragmentation while other
   * streams have queued fragments and its own stream has none. The frame goes
   * downstream at once if its data is at most this size. A larger frame waits
   * for the next flush, which sends it before the queued fragments.
   *
   * This value does not stop fragmentation. A frame whose data does not fit in
   * its first fragment is split, however small its data is.
   *
   * Default: 1KB.
   */
  size_t minSizeToFragment{1024};
};

} // namespace apache::thrift::fast_thrift::frame::write
