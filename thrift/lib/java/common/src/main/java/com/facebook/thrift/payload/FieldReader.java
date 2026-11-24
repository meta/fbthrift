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

package com.facebook.thrift.payload;

import java.util.Objects;
import java.util.function.Supplier;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

/**
 * Reads one parameter of an interface method: its Field ID, wire type, value reader, and a supplier
 * of the default-constructed value it takes when the request omits it. This is the Java counterpart
 * of the per-parameter {@code FieldData} in C++ generated {@code pargs}, which lets the server
 * decode the arguments by Field ID.
 */
public final class FieldReader<T> implements Reader<T> {
  private final short fieldId;
  private final byte fieldType;
  private final Reader<T> delegate;
  private final Supplier<T> defaultValue;

  public FieldReader(short fieldId, byte fieldType, Reader<T> delegate, Supplier<T> defaultValue) {
    this.fieldId = fieldId;
    this.fieldType = fieldType;
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.defaultValue = Objects.requireNonNull(defaultValue, "defaultValue");
  }

  public short getFieldId() {
    return fieldId;
  }

  public byte getFieldType() {
    return fieldType;
  }

  /** Returns a new default value, so no two requests share one. */
  public T getDefaultValue() {
    return defaultValue.get();
  }

  @Override
  public T read(TProtocol protocol) throws TException {
    return delegate.read(protocol);
  }
}
