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

#include <folly/Range.h>

namespace apache::thrift {

/**
 * Per-request encryption state — set by the rocket server's request dispatch
 * logic and logged to perfpipe_thrift_request_events.request_encryption_state.
 *
 * Designed to be the canonical "is this request encrypted?" signal for ALL
 * thrift requests, regardless of underlying transport. Distinguishes the
 * StopTLSv2 case (where encryption is per-record and may flip to plaintext
 * mid-connection) from standard encrypted/plaintext connections.
 *
 * Default is Plaintext to fail-safe — consumers should treat unset as "not
 * encrypted" for security-sensitive analyses.
 */
enum class RequestEncryptionState {
  // Connection has no encryption (plaintext) OR security protocol is unknown.
  // Default value to fail safely.
  Plaintext = 0,
  // Standard encryption (TLS, Fizz, Fizz/KTLS, thriftPSPV0).
  Encrypted = 1,
  // StopTLSv2 connection, no plaintext records observed yet on this connection.
  StoptlsEncrypted = 2,
  // StopTLSv2 connection, plaintext records have been observed at some point
  // on this connection.
  StoptlsSkipped = 3,
};

/**
 * Returns the canonical Scuba string for a RequestEncryptionState value:
 *   "plaintext", "encrypted", "stoptls_encrypted", "stoptls_skipped"
 */
folly::StringPiece toString(RequestEncryptionState state);

} // namespace apache::thrift
