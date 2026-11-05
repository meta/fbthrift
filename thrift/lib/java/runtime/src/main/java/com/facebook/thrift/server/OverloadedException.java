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

package com.facebook.thrift.server;

import javax.annotation.Nullable;
import org.apache.thrift.TApplicationException;

/**
 * A {@link TApplicationException#LOADSHEDDING} rejection that also names the reason the C++ server
 * reports for the same cause, so Service Router classifies the response as host or app overload
 * instead of ignoring a bare body.
 *
 * <p>Reasons mirror {@code thrift/lib/cpp2/async/ResponseChannel.h} and are intentionally distinct
 * from the body type {@link TApplicationException#LOADSHEDDING}: {@link #HOST_OVERLOAD} is generic
 * host admission (active-request and QPS limits, undifferentiated scheduler rejection); {@link
 * #QUEUE_OVERLOAD} exists only for a proven bounded-queue rejection; {@link #APP_OVERLOAD} is a
 * custom application overload decision. An undifferentiated {@code RejectedExecutionException}
 * (interruption and shutdown reject too) must stay {@link #HOST_OVERLOAD}.
 *
 * <p>The reason travels as an {@code int} and is rendered to the THeader {@code ex} wire header at
 * exactly one place, the header-only error encoder. This class performs no wire encoding and no
 * string comparison.
 */
public final class OverloadedException extends TApplicationException {

  /** Generic host admission, C++ {@code kOverloadedErrorCode}. */
  public static final int HOST_OVERLOAD = 1;

  /** Proven bounded-queue rejection only, C++ {@code kQueueOverloadedErrorCode}. */
  public static final int QUEUE_OVERLOAD = 5;

  /** Custom application overload decision, C++ {@code kAppOverloadedErrorCode}. */
  public static final int APP_OVERLOAD = 22;

  /** No overload reason. Never encode or map this value. */
  public static final int NO_RPC_ERROR_CODE = -1;

  private final int rpcErrorCode;

  private OverloadedException(int rpcErrorCode, String message) {
    super(TApplicationException.LOADSHEDDING, message);
    this.rpcErrorCode = rpcErrorCode;
  }

  /** Generic host admission overload with the given message. */
  public static OverloadedException host(String message) {
    return new OverloadedException(HOST_OVERLOAD, message);
  }

  /** Proven bounded-queue overload with the given message. */
  public static OverloadedException queue(String message) {
    return new OverloadedException(QUEUE_OVERLOAD, message);
  }

  /** Custom application overload decision with the given message. */
  public static OverloadedException application(String message) {
    return new OverloadedException(APP_OVERLOAD, message);
  }

  /** The reason code the C++ server reports for the same cause. */
  public int getRpcErrorCode() {
    return rpcErrorCode;
  }

  /**
   * One transport-neutral reason for an overload: 1, 5, or 22; the constant otherwise. Header
   * writes it as {@code ex}, Rocket maps it to {@code ResponseRpcErrorCode} (C++ {@code
   * RocketThriftRequests.cpp} table).
   */
  public static int rpcErrorCodeOf(@Nullable TApplicationException exception) {
    if (exception instanceof OverloadedException) {
      return ((OverloadedException) exception).getRpcErrorCode();
    }
    if (exception != null && exception.getType() == TApplicationException.LOADSHEDDING) {
      return APP_OVERLOAD;
    }
    return NO_RPC_ERROR_CODE;
  }
}
