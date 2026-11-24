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

package main

import (
	"context"
	"errors"
	"time"

	"apache/thrift/test/load"
)

// LoadTestHandler encapsulates parameters for your service processor
type LoadTestHandler struct{}

// Compile-time interface enforcer
var _ load.LoadTest = (*LoadTestHandler)(nil)

// Echo - does echo
func (h *LoadTestHandler) Echo(ctx context.Context, in []byte) ([]byte, error) {
	return in, nil
}

// Add - adds
func (h *LoadTestHandler) Add(ctx context.Context, a int64, b int64) (int64, error) {
	return a + b, nil
}

// Noop - a noop
func (h *LoadTestHandler) Noop(ctx context.Context) error {
	return nil
}

// OnewayNoop - a one way noop
func (h *LoadTestHandler) OnewayNoop(ctx context.Context) error {
	return nil
}

// AsyncNoop - a async noop
func (h *LoadTestHandler) AsyncNoop(ctx context.Context) error {
	return nil
}

// Sleep - a sleep
func (h *LoadTestHandler) Sleep(ctx context.Context, microseconds int64) error {
	time.Sleep(time.Duration(microseconds) * time.Microsecond)
	return nil
}

// OnewaySleep - a oneway sleep
func (h *LoadTestHandler) OnewaySleep(ctx context.Context, microseconds int64) error {
	time.Sleep(time.Duration(microseconds) * time.Microsecond)
	return nil
}

// Burn - a time burn
func (h *LoadTestHandler) Burn(ctx context.Context, microseconds int64) error {
	burnImpl(microseconds)
	return nil
}

// OnewayBurn - a one way time burn
func (h *LoadTestHandler) OnewayBurn(ctx context.Context, microseconds int64) error {
	burnImpl(microseconds)
	return nil
}

// BadSleep - a burn instead of a sleep
func (h *LoadTestHandler) BadSleep(ctx context.Context, microseconds int64) error {
	burnImpl(microseconds)
	return nil
}

// BadBurn - a time burn
func (h *LoadTestHandler) BadBurn(ctx context.Context, microseconds int64) error {
	burnImpl(microseconds)
	return nil
}

// ThrowError - throws a LoadError
func (h *LoadTestHandler) ThrowError(ctx context.Context, code int32) error {
	return load.NewLoadError()
}

// ThrowUnexpected - throws a ApplicationException
func (h *LoadTestHandler) ThrowUnexpected(ctx context.Context, code int32) error {
	return errors.New("Unknown Exception")
}

// OnewayThrow - throws a ApplicationException
func (h *LoadTestHandler) OnewayThrow(ctx context.Context, code int32) error {
	return errors.New("Unknown Exception")
}

// Send - does nothing
func (h *LoadTestHandler) Send(ctx context.Context, data []byte) error {
	return nil
}

// OnewaySend - does nothing
func (h *LoadTestHandler) OnewaySend(ctx context.Context, data []byte) error {
	return nil
}

// Recv - returns a byte array of the requested size filled with 'a'
func (h *LoadTestHandler) Recv(ctx context.Context, bytes int64) ([]byte, error) {
	res := make([]byte, bytes)
	for n := range res {
		res[n] = byte('a')
	}
	return res, nil
}

// Sendrecv - returns a Recv invocation
func (h *LoadTestHandler) Sendrecv(ctx context.Context, data []byte, recvBytes int64) ([]byte, error) {
	return h.Recv(ctx, recvBytes)
}

// LargeContainer - does nothing
func (h *LoadTestHandler) LargeContainer(ctx context.Context, items []*load.BigStruct) error {
	return nil
}

// IterAllFields - iterates over all items and their fields
func (h *LoadTestHandler) IterAllFields(ctx context.Context, items []*load.BigStruct) ([]*load.BigStruct, error) {
	for _, item := range items {
		_ = item.GetStringField()
		for range item.GetStringList() {
		}
	}

	return items, nil
}

func burnImpl(microseconds int64) {
	end := time.Now().UnixNano() + microseconds*1000
	for time.Now().UnixNano() < end {
	}
}
