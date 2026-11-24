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

import org.apache.thrift.TApplicationException;

/**
 * A request names a service or method the server does not serve. The Rocket transport reports it as
 * an INVALID frame so callers distinguish a bad method from a failed one; Header transports send it
 * as an ordinary unknown-method body.
 */
public final class UnknownMethodException extends TApplicationException {
  private static final long serialVersionUID = 1L;

  public UnknownMethodException(String name) {
    super(TApplicationException.UNKNOWN_METHOD, "Method name " + name + " not found");
  }
}
