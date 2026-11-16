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

package com.facebook.thrift.util;

/**
 * Wire constants of the THeader transport and of the Thrift response error headers. Names and
 * values follow the C++ runtime, so that both sides read the same: {@code
 * thrift/lib/cpp/transport/THeader.h} (framing), {@code
 * thrift/lib/cpp2/transport/core/RpcMetadataUtil.h} (header names), and {@code
 * thrift/lib/cpp2/async/ResponseChannel.cpp} (error codes).
 */
public final class ThriftProtocolConstants {
  private ThriftProtocolConstants() {}

  // THeader framing

  /** Largest THeader frame that a 4-byte length can describe, length prefix excluded. */
  public static final int MAX_FRAME_SIZE = 0x3FFFFFFF;

  /** Replaces the 4-byte length of a frame larger than {@link #MAX_FRAME_SIZE} ("BIGF"). */
  public static final int BIG_FRAME_MAGIC = 0x42494746;

  /** First 16 bits of every THeader frame; C++ keeps it shifted left by 16 bits. */
  public static final int HEADER_MAGIC = 0x0FFF;

  public static final int HEADER_FLAGS_NONE = 0x00;
  public static final int HEADER_FLAG_SUPPORT_OUT_OF_ORDER = 0x01;

  /** Info ID of a block of normal headers (C++ {@code infoIdType::KEYVALUE}). */
  public static final int NORMAL_HEADERS = 0x01;

  /** Info ID of a block of persistent headers (C++ {@code infoIdType::PKEYVALUE}). */
  public static final int PERSISTENT_HEADERS = 0x02;

  /**
   * Reserved frame sequence marking a Header oneway request (C++ {@code
   * ResponseChannel::ONEWAY_REQUEST_ID}, UINT32_MAX read as -1). A request frame carrying this
   * sequence gets no response, even with a regular T_CALL body. This is a frame-level signal,
   * distinct from the legacy body message sequence -1 rule.
   */
  public static final int FRAME_NO_REPLY_SEQ_ID = -1;

  // Response error headers. Service Router reads the proxied anyex pair first, then anyex, then
  // uex, then ex.

  public static final String HEADER_EX = "ex";
  public static final String HEADER_PROXIED_EX = "pex";
  public static final String HEADER_EX_META = "exm";
  public static final String HEADER_UEX = "uex";
  public static final String HEADER_PROXIED_UEX = "puex";
  public static final String HEADER_UEXW = "uexw";
  public static final String HEADER_PROXIED_UEXW = "puexw";
  public static final String HEADER_ANYEX = "anyex";
  public static final String HEADER_PROXIED_ANYEX = "panyex";
  public static final String HEADER_ANYEX_TYPE = "anyext";
  public static final String HEADER_PROXIED_ANYEX_TYPE = "panyext";

  // Error codes in the ex header

  public static final String UNKNOWN_ERROR_CODE = "0";
  public static final String OVERLOADED_ERROR_CODE = "1";
  public static final String TASK_EXPIRED_ERROR_CODE = "2";
  public static final String QUEUE_OVERLOADED_ERROR_CODE = "5";
  public static final String INJECTED_FAILURE_ERROR_CODE = "14";
  public static final String SERVER_QUEUE_TIMEOUT_ERROR_CODE = "15";
  public static final String RESPONSE_TOO_BIG_ERROR_CODE = "17";
  public static final String WRONG_RPC_KIND_ERROR_CODE = "21";
  public static final String APP_OVERLOADED_ERROR_CODE = "22";
  public static final String METHOD_UNKNOWN_ERROR_CODE = "25";
  public static final String INTERACTION_ID_UNKNOWN_ERROR_CODE = "26";
  public static final String INTERACTION_CONSTRUCTOR_ERROR_ERROR_CODE = "27";
  public static final String REQUEST_PARSING_ERROR_CODE = "28";
  public static final String UNIMPLEMENTED_METHOD_ERROR_CODE = "31";
  public static final String TENANT_QUOTA_EXCEEDED_ERROR_CODE = "32";
  public static final String INTERACTION_LOADSHEDDED_ERROR_CODE = "34";
  public static final String INTERACTION_LOADSHEDDED_QUEUE_TIMEOUT_ERROR_CODE = "35";
  public static final String INTERACTION_LOADSHEDDED_OVERLOAD_ERROR_CODE = "36";
  public static final String INTERACTION_LOADSHEDDED_APP_OVERLOAD_ERROR_CODE = "37";
}
