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
	"iter"
	"math"
	"net"
	"runtime"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift/rocket"
	"github.com/facebook/fbthrift/thrift/lib/go/thrift/types"
	"github.com/facebook/fbthrift/thrift/lib/thrift/rpcmetadata"
	"github.com/jjeffcaii/reactor-go/scheduler"
	rsocket "github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/core"
	"github.com/rsocket/rsocket-go/core/transport"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"
)

type rsocketClient struct {
	client rsocket.Client
	conn   net.Conn

	clientScheduler scheduler.Scheduler

	initGroup singleflight.Group

	protoID rpcmetadata.ProtocolId
}

func newRSocketClient(conn net.Conn, protoID rpcmetadata.ProtocolId) *rsocketClient {
	return &rsocketClient{
		conn:            conn,
		clientScheduler: scheduler.NewElastic(math.MaxInt32),
		protoID:         protoID,
	}
}

func (r *rsocketClient) SendSetup(_ context.Context) error {
	// Only a single instance of the code below is allowed to run at a time.
	// If multiple goroutines arrive at this point concurrently - they will
	// all wait here for a single instance of "setup" to complete.
	// If "setup" has already run (client != nil) - we return immediately.
	// If "setup" has already run but failed with an error (client == nil),
	// the code below will be retried by the next goroutine that arrives here.
	_, setupErr, _ := r.initGroup.Do("setup", func() (any, error) {
		if r.client != nil {
			return nil, nil
		}

		setupPayload, err := rocket.NewRequestSetupPayloadVersion8()
		if err != nil {
			return nil, err
		}
		// Very important to reset the deadline! Especially when using UpgradeToRocket.
		// We may have inherited this connection from Header protocol after an Upgrade.
		// Deadlines may be nearing expiration, if not reset - rsocket setup may fail.
		r.resetDeadline()
		// See T182939211. This copies the keep alives from Java Rocket.
		// KeepaliveLifetime = time.Duration(missedAcks = 1) * (ackTimeout = 3600000)
		clientBuilder := rsocket.Connect().
			KeepAlive(time.Millisecond*30000, time.Millisecond*3600000, 1).
			MetadataMimeType(rocket.RocketMetadataCompactMimeType).
			Scheduler(r.clientScheduler, r.clientScheduler).
			SetupPayload(setupPayload).
			OnClose(func(error) {})

		clientStarter := clientBuilder.Acceptor(
			func(_ context.Context, _ rsocket.RSocket) rsocket.RSocket {
				return rsocket.NewAbstractSocket(
					rsocket.MetadataPush(
						r.onServerMetadataPush,
					),
				)
			},
		)

		client, err := clientStarter.Transport(transporter(r.conn)).Start(context.Background())
		if err != nil {
			return nil, err
		}
		r.client = client
		return nil, nil
	})

	return setupErr
}

func (r *rsocketClient) onServerMetadataPush(pay payload.Payload) {
	_, err := rocket.DecodeServerMetadataPush(pay)
	if err != nil {
		panic(err)
	}
}

func transporter(conn net.Conn) func(_ context.Context) (*transport.Transport, error) {
	return func(_ context.Context) (*transport.Transport, error) {
		tconn := transport.NewTCPClientTransport(conn)
		tconn.SetLifetime(1 * time.Hour)
		return tconn, nil
	}
}

func (r *rsocketClient) resetDeadline() {
	// It is necessary to reset the deadline to 0.
	// The rsocket library only sets the deadline at connection start.
	// This means if you wait long enough, the connection will become useless.
	// Or something else is happening, but this is very necessary.
	r.conn.SetDeadline(time.Time{})
}

func setReadHeaders(ctx context.Context, headers map[string]string) {
	rpcOpts := GetRPCOptions(ctx)
	if rpcOpts != nil {
		rpcOpts.SetReadHeaders(headers)
	}
}

func (r *rsocketClient) prepareRequestPayload(
	ctx context.Context,
	messageName string,
	headers map[string]string,
	request WritableStruct,
	rpcKind rpcmetadata.RpcKind,
) (payload.Payload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dataBytes, err := encodeRequest(r.protoID, request)
	if err != nil {
		return nil, err
	}
	if err := r.SendSetup(ctx); err != nil {
		return nil, err
	}
	r.resetDeadline()
	return rocket.EncodeRequestPayload(
		ctx,
		messageName,
		r.protoID,
		rpcKind,
		headers,
		rpcmetadata.CompressionAlgorithm_NONE,
		dataBytes,
	)
}

func (r *rsocketClient) processResponsePayload(
	ctx context.Context,
	firstPayload payload.Payload,
	response ReadableResult,
) error {
	firstRespPayload, err := rocket.DecodeResponsePayload(firstPayload)
	if err != nil {
		return err
	}
	setReadHeaders(ctx, firstRespPayload.Headers())
	return decodeResultOrException(r.protoID, firstRespPayload.Data(), response)
}

func (r *rsocketClient) RequestResponse(
	ctx context.Context,
	messageName string,
	headers map[string]string,
	request WritableStruct,
	response ReadableResult,
) error {
	reqPayload, err := r.prepareRequestPayload(
		ctx,
		messageName,
		headers,
		request,
		rpcmetadata.RpcKind_SINGLE_REQUEST_SINGLE_RESPONSE,
	)
	if err != nil {
		return err
	}
	mono := r.client.RequestResponse(reqPayload)
	val, err := mono.Block(ctx)
	if err != nil {
		return decodeResponseError(err)
	}
	return r.processResponsePayload(ctx, val, response)
}

func (r *rsocketClient) FireAndForget(ctx context.Context, messageName string, headers map[string]string, request WritableStruct) error {
	reqPayload, err := r.prepareRequestPayload(
		ctx,
		messageName,
		headers,
		request,
		rpcmetadata.RpcKind_SINGLE_REQUEST_NO_RESPONSE,
	)
	if err != nil {
		return err
	}
	r.client.FireAndForget(reqPayload)
	return nil
}

func (r *rsocketClient) RequestStream(
	ctx context.Context,
	messageName string,
	headers map[string]string,
	request WritableStruct,
	response ReadableResult,
	newStreamElemFn func() ReadableResult,
) (StreamingHandle[ReadableStruct], error) {
	reqPayload, err := r.prepareRequestPayload(
		ctx,
		messageName,
		headers,
		request,
		rpcmetadata.RpcKind_SINGLE_REQUEST_STREAMING_RESPONSE,
	)
	if err != nil {
		return nil, err
	}

	flux := r.client.RequestStream(reqPayload)

	// Split semantics: ctx governs the initial response only. The stream
	// phase is detached and owned by the handle's Cancel/cleanup.
	streamCtx, streamCancel := context.WithCancel(context.WithoutCancel(ctx))
	streamPayloadChan, streamErrChan := flux.ToChan(streamCtx, types.DefaultStreamBufferSize)

	firstPayload, err := recvStreamNext(ctx, streamPayloadChan, streamErrChan)
	if err != nil {
		streamCancel()
		return nil, decodeResponseError(err)
	}
	err = r.processResponsePayload(ctx, firstPayload, response)
	if err != nil {
		streamCancel()
		return nil, err
	}

	ch := make(chan types.ChanResult[types.ReadableStruct], types.DefaultStreamBufferSize)
	go func() {
		defer runtime.KeepAlive(r)
		defer close(ch)
		// send delivers one stream element or terminal error, mirroring
		// yield's (elem, err) shape. It returns false if the stream was
		// cancelled before the value could be delivered.
		send := func(elem types.ReadableStruct, err error) bool {
			select {
			case ch <- types.ChanResult[types.ReadableStruct]{Elem: elem, Err: err}:
				return true
			case <-streamCtx.Done():
				return false
			}
		}

		for {
			streamPayload, streamErr := recvStreamNext(streamCtx, streamPayloadChan, streamErrChan)
			if streamErr != nil {
				send(nil, streamErr)
				return
			} else if streamPayload != nil {
				streamResponse, err := rocket.DecodeStreamPayload(streamPayload)
				if err != nil {
					send(nil, err)
					return
				}
				data := streamResponse.Data()
				destStruct := newStreamElemFn()
				err = decodeResultOrException(r.protoID, data, destStruct)
				if err != nil {
					send(nil, err)
					return
				}

				if !send(destStruct, nil) {
					return
				}
			} else {
				// Stream completion
				return
			}
		}
	}()

	return types.NewStreamingHandle(ch, streamCancel), nil
}

func (r *rsocketClient) MetadataPush(_ context.Context, metadata *rpcmetadata.ClientPushMetadata) error {
	r.resetDeadline()
	payload, err := rocket.EncodePayloadMetadataAndData(metadata, nil, 0)
	if err != nil {
		return err
	}
	r.client.MetadataPush(payload)
	return nil
}

func (r *rsocketClient) RequestSink(
	ctx context.Context,
	messageName string,
	headers map[string]string,
	request WritableStruct,
	firstResponse ReadableResult,
) (func(sinkSeq iter.Seq2[WritableResult, error], finalResponse ReadableResult) error, error) {
	reqPayload, err := r.prepareRequestPayload(
		ctx,
		messageName,
		headers,
		request,
		rpcmetadata.RpcKind_SINK,
	)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Create a flux that yields the initial request payload and then waits for sink items
	sinkPayloadChan := make(chan payload.Payload, types.DefaultStreamBufferSize)
	sendingDoneChan := make(chan struct{}, 1)
	sendingFlux := flux.Create(func(fluxCtx context.Context, sink flux.Sink) {
		defer close(sendingDoneChan)

		// Send payloads from the sink channel until it's closed or context is done
		for {
			select {
			case p, ok := <-sinkPayloadChan:
				if !ok {
					// Channel closed, complete the flux
					sink.Complete()
					return
				}
				sink.Next(p)
			case <-fluxCtx.Done():
				sink.Error(fluxCtx.Err())
				return
			}
		}
	})

	receivingFlux := r.client.RequestChannel(reqPayload, sendingFlux)

	channelCtx, channelCancel := context.WithCancel(ctx)
	receivingPayloadChan, receivingErrChan := receivingFlux.ToChan(channelCtx, types.DefaultStreamBufferSize)

	// Receive the first response payload
	firstPayload, err := recvStreamNext(channelCtx, receivingPayloadChan, receivingErrChan)
	if err != nil {
		channelCancel()
		close(sinkPayloadChan)
		waitForRequestChannelError(err, sendingDoneChan)
		return nil, decodeResponseError(err)
	}
	err = r.processResponsePayload(ctx, firstPayload, firstResponse)
	if err != nil {
		channelCancel()
		close(sinkPayloadChan)
		<-sendingDoneChan
		return nil, err
	}

	// Create sink callback that will be called by the user to send sink items
	sinkCallback := func(sinkSeq iter.Seq2[WritableResult, error], finalResponse ReadableResult) error {
		defer channelCancel()

		sendAllSinkItems := func() error {
			defer func() {
				close(sinkPayloadChan)
				<-sendingDoneChan
			}()

			for item, itemErr := range sinkSeq {
				var finalErr error
				payloadMetadata := rpcmetadata.NewPayloadMetadata()
				if itemErr != nil {
					exceptionMetadataBase := rocket.NewPayloadExceptionMetadataBaseV2(itemErr)
					payloadMetadata.SetExceptionMetadata(exceptionMetadataBase)
					finalErr = itemErr
				} else if declaredException := item.Exception(); declaredException != nil {
					exceptionMetadataBase := rocket.NewPayloadExceptionMetadataBaseV2(declaredException)
					payloadMetadata.SetExceptionMetadata(exceptionMetadataBase)
					finalErr = declaredException
				} else {
					responseMetadata := rpcmetadata.NewPayloadResponseMetadata()
					payloadMetadata.SetResponseMetadata(responseMetadata)
				}

				metadata := rpcmetadata.NewStreamPayloadMetadata().
					SetCompression(Pointerize(rpcmetadata.CompressionAlgorithm_NONE)).
					SetPayloadMetadata(payloadMetadata)

				// Encode the sink item
				var itemBytes []byte
				if item != nil {
					itemBytes, err = encodeRequest(r.protoID, item)
					if err != nil {
						return err
					}
				}

				sinkPayload, err := rocket.EncodePayloadMetadataAndData(metadata, itemBytes, rpcmetadata.CompressionAlgorithm_NONE)
				if err != nil {
					return err
				}

				select {
				case sinkPayloadChan <- sinkPayload:
				case <-ctx.Done():
					return ctx.Err()
				}

				if finalErr != nil {
					return finalErr
				}
			}
			return nil
		}
		err = sendAllSinkItems()
		if err != nil {
			return err
		}

		// Wait for the final response
		finalPayload, err := recvStreamNext(channelCtx, receivingPayloadChan, receivingErrChan)
		if err != nil {
			return err
		}
		if finalPayload == nil {
			return fmt.Errorf("expected final response but channel completed")
		}

		finalRespPayload, err := rocket.DecodeStreamPayload(finalPayload)
		if err != nil {
			return err
		}

		// Decode the final response
		return decodeResultOrException(r.protoID, finalRespPayload.Data(), finalResponse)
	}

	return sinkCallback, nil
}

func (r *rsocketClient) RequestBiDiStream(
	ctx context.Context,
	messageName string,
	headers map[string]string,
	request WritableStruct,
	firstResponse ReadableResult,
	newStreamElemFn func() ReadableResult,
) (func(sinkSeq iter.Seq2[WritableResult, error]), iter.Seq2[ReadableStruct, error], error) {
	reqPayload, err := r.prepareRequestPayload(
		ctx,
		messageName,
		headers,
		request,
		rpcmetadata.RpcKind_BIDIRECTIONAL_STREAM,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	// Create a flux for sending sink items to the server
	sinkPayloadChan := make(chan payload.Payload, types.DefaultStreamBufferSize)
	sendingDoneChan := make(chan struct{}, 1)
	sendingFlux := flux.Create(func(fluxCtx context.Context, sink flux.Sink) {
		defer close(sendingDoneChan)

		// Send payloads from the sink channel until it's closed or context is done
		for {
			select {
			case p, ok := <-sinkPayloadChan:
				if !ok {
					// Channel closed, complete the flux
					sink.Complete()
					return
				}
				sink.Next(p)
			case <-fluxCtx.Done():
				sink.Error(fluxCtx.Err())
				return
			}
		}
	})

	receivingFlux := r.client.RequestChannel(reqPayload, sendingFlux)

	channelCtx, channelCancel := context.WithCancel(ctx)
	receivingPayloadChan, receivingErrChan := receivingFlux.ToChan(channelCtx, types.DefaultStreamBufferSize)

	// Receive the first response payload
	firstPayload, err := recvStreamNext(channelCtx, receivingPayloadChan, receivingErrChan)
	if err != nil {
		channelCancel()
		close(sinkPayloadChan)
		waitForRequestChannelError(err, sendingDoneChan)
		return nil, nil, decodeResponseError(err)
	}
	err = r.processResponsePayload(ctx, firstPayload, firstResponse)
	if err != nil {
		channelCancel()
		close(sinkPayloadChan)
		<-sendingDoneChan
		return nil, nil, err
	}

	// Create sink callback for sending items to the server (no final response in BiDi)
	sinkCallback := func(sinkSeq iter.Seq2[WritableResult, error]) {
		defer func() {
			close(sinkPayloadChan)
			<-sendingDoneChan
		}()

		for item, itemErr := range sinkSeq {
			var finalErr error
			payloadMetadata := rpcmetadata.NewPayloadMetadata()
			if itemErr != nil {
				exceptionMetadataBase := rocket.NewPayloadExceptionMetadataBaseV2(itemErr)
				payloadMetadata.SetExceptionMetadata(exceptionMetadataBase)
				finalErr = itemErr
			} else if declaredException := item.Exception(); declaredException != nil {
				exceptionMetadataBase := rocket.NewPayloadExceptionMetadataBaseV2(declaredException)
				payloadMetadata.SetExceptionMetadata(exceptionMetadataBase)
				finalErr = declaredException
			} else {
				responseMetadata := rpcmetadata.NewPayloadResponseMetadata()
				payloadMetadata.SetResponseMetadata(responseMetadata)
			}

			metadata := rpcmetadata.NewStreamPayloadMetadata().
				SetCompression(Pointerize(rpcmetadata.CompressionAlgorithm_NONE)).
				SetPayloadMetadata(payloadMetadata)

			// Encode the sink item
			var itemBytes []byte
			if item != nil {
				itemBytes, err = encodeRequest(r.protoID, item)
				if err != nil {
					return
				}
			}

			sinkPayload, err := rocket.EncodePayloadMetadataAndData(metadata, itemBytes, rpcmetadata.CompressionAlgorithm_NONE)
			if err != nil {
				return
			}

			select {
			case sinkPayloadChan <- sinkPayload:
			case <-channelCtx.Done():
				return
			}

			if finalErr != nil {
				return
			}
		}
	}

	// Create stream iterator for receiving items from the server
	streamSeq := func(yield func(ReadableStruct, error) bool) {
		defer channelCancel()

		for {
			streamPayload, streamErr := recvStreamNext(channelCtx, receivingPayloadChan, receivingErrChan)
			if streamErr != nil {
				yield(nil, streamErr)
				return
			} else if streamPayload != nil {
				streamResponse, err := rocket.DecodeStreamPayload(streamPayload)
				if err != nil {
					yield(nil, err)
					return
				}
				data := streamResponse.Data()
				destStruct := newStreamElemFn()
				err = decodeResultOrException(r.protoID, data, destStruct)
				if err != nil {
					yield(nil, err)
					return
				}

				if !yield(destStruct, nil) {
					return
				}
			} else {
				// Stream completion
				return
			}
		}
	}

	return sinkCallback, streamSeq, nil
}

func (r *rsocketClient) Close() error {
	defer r.clientScheduler.Close()
	if r.client != nil {
		return r.client.Close()
	}
	return r.conn.Close()
}

func recvStreamNext(ctx context.Context, streamPayloadChan <-chan payload.Payload, streamErrChan <-chan error) (payload.Payload, error) {
	// The logic below allows us to deal with channel-close race conditions
	// in a graceful manner. It's possible for any (or both) of the channels
	// to get closed before all the bufferred data is received from them.
	// The challenge is to figure out if the other channel still has data
	// to receive if one channel becomes closed. We achieve this by setting
	// a channel to nil (nil = not ready) and performing another iteration.
	// (https://stackoverflow.com/a/13666733)

	for range 2 /* max 2 iterations */ {
		select {
		case streamPayload, ok := <-streamPayloadChan:
			if ok {
				return streamPayload, nil
			}
			streamPayloadChan = nil
		case err, ok := <-streamErrChan:
			if ok {
				return nil, err
			}
			streamErrChan = nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// (nil, nil) indicates completion - i.e. both channels confirmed closed.
	return nil, ctx.Err()
}

// A per-request ERROR frame proves the initial request reached the peer, so
// rsocket-go has scheduled the sending flux. Join its completion before a caller
// can close the connection. Cancellation and connection errors may precede
// subscription and must not wait for a sender that never started.
func waitForRequestChannelError(err error, sendingDone <-chan struct{}) {
	if frame, ok := errors.AsType[core.CustomError](err); ok {
		switch frame.ErrorCode() {
		case core.ErrorCodeInvalid, core.ErrorCodeRejected, core.ErrorCodeCanceled, core.ErrorCodeApplicationError:
			<-sendingDone
		}
	}
}

func encodeRequest(protoID rpcmetadata.ProtocolId, request WritableStruct) ([]byte, error) {
	switch protoID {
	case rpcmetadata.ProtocolId_BINARY:
		return EncodeBinary(request)
	case rpcmetadata.ProtocolId_COMPACT:
		return EncodeCompact(request)
	default:
		return nil, types.NewProtocolException(fmt.Errorf("Unknown protocol id: %d", protoID))
	}
}

func decodeResultOrException(protoID rpcmetadata.ProtocolId, data []byte, result ReadableResult) error {
	var err error
	switch protoID {
	case rpcmetadata.ProtocolId_BINARY:
		err = DecodeBinary(data, result)
	case rpcmetadata.ProtocolId_COMPACT:
		err = DecodeCompact(data, result)
	default:
		err = types.NewProtocolException(fmt.Errorf("Unknown protocol id: %d", protoID))
	}
	if err != nil {
		return err
	}
	// Declared exception (inside the response)
	if exception := result.Exception(); exception != nil {
		return exception
	}
	return nil
}
