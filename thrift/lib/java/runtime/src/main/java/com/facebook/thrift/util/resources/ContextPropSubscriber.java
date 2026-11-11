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

package com.facebook.thrift.util.resources;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

/**
 * @deprecated Forwards every signal to its delegate and propagates nothing. Reactive code reads the
 *     request context with {@link com.facebook.nifty.core.RequestContexts#currentContext()}. Kept
 *     so callers compiled against it still link.
 */
@Deprecated
public class ContextPropSubscriber<T> implements CoreSubscriber<T> {
  private final CoreSubscriber<T> delegate;

  public ContextPropSubscriber(CoreSubscriber<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Context currentContext() {
    return delegate.currentContext();
  }

  @Override
  public void onSubscribe(Subscription s) {
    delegate.onSubscribe(s);
  }

  @Override
  public void onNext(T t) {
    delegate.onNext(t);
  }

  @Override
  public void onError(Throwable t) {
    delegate.onError(t);
  }

  @Override
  public void onComplete() {
    delegate.onComplete();
  }
}
