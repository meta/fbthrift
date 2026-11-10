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

package thrift

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift/dummy"
	"github.com/facebook/fbthrift/thrift/lib/go/thrift/types"
	"github.com/facebook/fbthrift/thrift/lib/thrift/rpcmetadata"
	dummyif "github.com/facebook/fbthrift/thrift/test/go/if/dummy"
	"github.com/rsocket/rsocket-go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestUnknownMethodAcrossProtocols(t *testing.T) {
	for _, proto := range []types.ProtocolID{types.ProtocolIDBinary, types.ProtocolIDCompact} {
		t.Run(fmt.Sprint(proto), func(t *testing.T) {
			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)
			server := NewServer(dummyif.NewDummyProcessor(&dummy.DummyHandler{}), listener, TransportIDRocket)
			ctx, cancel := context.WithCancel(t.Context())
			var group errgroup.Group
			group.Go(func() error { return server.ServeContext(ctx) })
			t.Cleanup(func() {
				cancel()
				require.ErrorIs(t, group.Wait(), context.Canceled)
			})
			channel, err := NewClient(WithRocket(), WithProtocolID(proto), WithIoTimeout(5*time.Second),
				WithDialer(func() (net.Conn, error) {
					return net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
				}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, channel.Close()) })
			client := dummyif.NewDummyChannelClient(channel)
			require.NoError(t, client.Ping(t.Context()))
			missing := dummyif.NewDummyTwoChannelClient(channel)
			err = missing.PingTwo(t.Context())
			appEx, ok := errors.AsType[*types.ApplicationException](err)
			require.True(t, ok)
			assert.EqualValues(t, types.UNKNOWN_METHOD, appEx.TypeID())
			assert.EqualError(t, appEx, "Method name PingTwo not found")
			require.NoError(t, client.Ping(t.Context()))
		})
	}
}

type responseErrorTestFrame struct {
	code core.ErrorCode
	data []byte
}

func (e *responseErrorTestFrame) Error() string             { return "test error frame" }
func (e *responseErrorTestFrame) ErrorCode() core.ErrorCode { return e.code }
func (e *responseErrorTestFrame) ErrorData() []byte         { return e.data }

func TestDecodeResponseErrorPreservesOtherErrors(t *testing.T) {
	require.EqualError(t, newUnknownMethodError("missingMethod"), "Method name missingMethod not found")
	unknown, err := EncodeCompact(rpcmetadata.NewResponseRpcError().
		SetCode(new(rpcmetadata.ResponseRpcErrorCode_UNKNOWN_METHOD)).
		SetWhatUTF8(new("Method name missingMethod not found")))
	require.NoError(t, err)
	parsing, err := EncodeCompact(rpcmetadata.NewResponseRpcError().
		SetCode(new(rpcmetadata.ResponseRpcErrorCode_REQUEST_PARSING_FAILURE)))
	require.NoError(t, err)
	for _, original := range []error{
		nil,
		errors.New("transport failed"),
		NewApplicationException(types.INTERNAL_ERROR, "handler failed"),
		&responseErrorTestFrame{code: core.ErrorCodeApplicationError, data: unknown},
		&responseErrorTestFrame{code: core.ErrorCodeInvalid, data: []byte{0xff}},
		&responseErrorTestFrame{code: core.ErrorCodeInvalid, data: []byte{0}},
		&responseErrorTestFrame{code: core.ErrorCodeInvalid, data: parsing},
	} {
		assert.Equal(t, original, decodeResponseError(original))
	}
	original := fmt.Errorf("wrapped: %w", &responseErrorTestFrame{code: core.ErrorCodeInvalid, data: unknown})
	mapped := decodeResponseError(original)
	appEx, ok := errors.AsType[*types.ApplicationException](mapped)
	require.True(t, ok)
	assert.EqualValues(t, types.UNKNOWN_METHOD, appEx.TypeID())
	assert.EqualError(t, appEx, "Method name missingMethod not found")
}
