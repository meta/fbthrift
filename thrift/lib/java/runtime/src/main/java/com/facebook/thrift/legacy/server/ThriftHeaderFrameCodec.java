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

package com.facebook.thrift.legacy.server;

import io.netty.channel.CombinedChannelDuplexHandler;

/**
 * THeader framing for one connection: {@link ThriftHeaderFrameLengthBasedDecoder} reads frames and
 * {@link ThriftHeaderFrameLengthPrepender} writes them, both with big frame ({@code BIGF}) support.
 */
public final class ThriftHeaderFrameCodec
    extends CombinedChannelDuplexHandler<
        ThriftHeaderFrameLengthBasedDecoder, ThriftHeaderFrameLengthPrepender> {

  /** Accepts each inbound frame up to the THeader limit for its kind. */
  public ThriftHeaderFrameCodec() {
    this(null);
  }

  /** Accepts inbound frames up to {@code maxFrameLength} bytes, length prefix included. */
  public ThriftHeaderFrameCodec(Integer maxFrameLength) {
    this(maxFrameLength, new ThriftHeaderFrameLengthPrepender());
  }

  ThriftHeaderFrameCodec(Integer maxFrameLength, ThriftHeaderFrameLengthPrepender prepender) {
    super(new ThriftHeaderFrameLengthBasedDecoder(maxFrameLength), prepender);
  }
}
