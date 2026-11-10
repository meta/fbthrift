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
	"maps"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift/dummy"
	"github.com/facebook/fbthrift/thrift/lib/go/thrift/types"
	dummyif "github.com/facebook/fbthrift/thrift/test/go/if/dummy"
	"github.com/rsocket/rsocket-go/core"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type channelErrorCountingHandler struct {
	*dummy.DummyHandler
	calls atomic.Int64
}

func (h *channelErrorCountingHandler) Ping(context.Context) error {
	h.calls.Add(1)
	return nil
}

type channelErrorProcessor struct{ Processor }

func (p channelErrorProcessor) ProcessorFunctionMap() map[string]ProcessorFunction {
	methods := maps.Clone(p.Processor.ProcessorFunctionMap())
	for _, name := range []string{"Echo", "StreamOnly", "SinkOnly", "BiDiBasic"} {
		delete(methods, name)
	}
	return methods
}

func TestRocketMissingMethodClientCleanup(t *testing.T) {
	for _, proto := range []types.ProtocolID{types.ProtocolIDBinary, types.ProtocolIDCompact} {
		t.Run(fmt.Sprint(proto), func(t *testing.T) {
			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)
			handler := &channelErrorCountingHandler{DummyHandler: &dummy.DummyHandler{}}
			processor := channelErrorProcessor{Processor: dummyif.NewDummyProcessor(handler)}
			server := NewServer(processor, listener, TransportIDRocket)
			ctx, cancel := context.WithCancel(t.Context())
			var group errgroup.Group
			group.Go(func() error { return server.ServeContext(ctx) })
			t.Cleanup(func() {
				cancel()
				require.ErrorIs(t, group.Wait(), context.Canceled)
			})
			calls := []struct {
				method string
				call   func(context.Context, dummyif.DummyClient) error
			}{
				{"Echo", func(ctx context.Context, client dummyif.DummyClient) error {
					_, err := client.Echo(ctx, "hello")
					return err
				}},
				{"StreamOnly", func(ctx context.Context, client dummyif.DummyClient) error {
					handle, err := client.StreamOnly(ctx, 1, 10)
					assert.Nil(t, handle)
					return err
				}},
				{"SinkOnly", func(ctx context.Context, client dummyif.DummyClient) error {
					callback, err := client.SinkOnly(ctx)
					assert.Nil(t, callback)
					return err
				}},
				{"BiDiBasic", func(ctx context.Context, client dummyif.DummyClient) error {
					callback, seq, err := client.BiDiBasic(ctx)
					assert.Nil(t, callback)
					assert.Nil(t, seq)
					return err
				}},
			}
			for _, call := range calls {
				t.Run(call.method, func(t *testing.T) {
					channel, err := NewClient(WithRocket(), WithProtocolID(proto), WithIoTimeout(5*time.Second),
						WithDialer(func() (net.Conn, error) {
							return net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
						}))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, channel.Close()) })
					client := dummyif.NewDummyChannelClient(channel)
					require.NoError(t, client.Ping(t.Context()))
					before := handler.calls.Load()
					err = call.call(t.Context(), client)
					appEx, ok := errors.AsType[*types.ApplicationException](err)
					require.True(t, ok, "%v", err)
					assert.EqualValues(t, types.UNKNOWN_METHOD, appEx.TypeID())
					assert.EqualError(t, appEx, "Method name "+call.method+" not found")
					assert.Equal(t, before, handler.calls.Load())
				})
			}
		})
	}
}

type cancelBeforeSubscribeRequest struct {
	WritableStruct
	cancel context.CancelFunc
}

func (r cancelBeforeSubscribeRequest) Write(encoder Encoder) error {
	r.cancel()
	return r.WritableStruct.Write(encoder)
}

func TestRocketChannelCancellationBeforeSubscribe(t *testing.T) {
	harness := setupTestServer(t, testServerConfig{timeoutDuration: 5 * time.Second})
	t.Cleanup(func() { harness.teardown(t) })
	addr := harness.server.(*server).listener.Addr()
	for _, kind := range []string{"sink", "bidi"} {
		t.Run(kind, func(t *testing.T) {
			channel, err := NewClient(WithRocket(), WithIoTimeout(5*time.Second),
				WithDialer(func() (net.Conn, error) {
					return net.DialTimeout(addr.Network(), addr.String(), 5*time.Second)
				}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, channel.Close()) })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			processor := dummyif.NewDummyProcessor(&dummy.DummyHandler{})
			request := cancelBeforeSubscribeRequest{
				WritableStruct: processor.ProcessorFunctionMap()["Ping"].NewReqArgs().(WritableStruct),
				cancel:         cancel,
			}
			extended := channel.(types.RequestChannelExtended)
			if kind == "sink" {
				callback, err := extended.SendRequestSink(ctx, "missingMethod", request, nil)
				assert.Nil(t, callback)
				require.ErrorIs(t, err, context.Canceled)
			} else {
				callback, seq, err := extended.SendRequestBiDi(ctx, "missingMethod", request, nil, nil)
				assert.Nil(t, callback)
				assert.Nil(t, seq)
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func TestRecvStreamNextClosedChannelsPreservesCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if cancelled {
				cancel()
			}
			payloads := make(chan payload.Payload)
			errCh := make(chan error)
			close(payloads)
			close(errCh)
			for range 100 {
				value, err := recvStreamNext(ctx, payloads, errCh)
				assert.Nil(t, value)
				if cancelled {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestWaitForRequestChannelErrorWaitsOnlyForPeerRefusals(t *testing.T) {
	for _, code := range []core.ErrorCode{
		core.ErrorCodeInvalid, core.ErrorCodeRejected, core.ErrorCodeCanceled, core.ErrorCodeApplicationError,
	} {
		t.Run(code.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				original := &responseErrorTestFrame{code: code}
				done := make(chan struct{})
				result := make(chan struct{})
				go func() {
					waitForRequestChannelError(original, done)
					close(result)
				}()
				synctest.Wait()
				select {
				case <-result:
					t.Fatal("returned before the sending flux finished")
				default:
				}
				close(done)
				<-result
			})
		})
	}
	for _, original := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		errors.New("connection failed"),
		&responseErrorTestFrame{code: core.ErrorCodeConnectionError},
		&responseErrorTestFrame{code: core.ErrorCodeInvalidSetup},
	} {
		waitForRequestChannelError(original, nil)
	}
}
