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

include "thrift/annotation/thrift.thrift"

@thrift.AllowLegacyMissingUris
package;

namespace cpp2 apache.thrift.conformance
namespace go thrift.conformance.rpc
namespace php apache_thrift
namespace py thrift.conformance.rpc
namespace py.asyncio thrift_asyncio.conformance.rpc
namespace py3 thrift.conformance
namespace java.swift org.apache.thrift.conformance

struct RpcTestCase {
  1: ClientInstruction clientInstruction;
  2: ClientTestResult clientTestResult;
  3: ServerInstruction serverInstruction;
  4: ServerTestResult serverTestResult;
}

struct Request {
  1: string data;
  2: optional i32 num;
}

struct Response {
  1: string data;
  2: optional i32 num;
}

exception UserException {
  1: string msg;
}

union ServerTestResult {
  1: RequestResponseBasicServerTestResult requestResponseBasic;
  2: RequestResponseDeclaredExceptionServerTestResult requestResponseDeclaredException;
  3: RequestResponseNoArgVoidResponseServerTestResult requestResponseNoArgVoidResponse;
  4: RequestResponseUndeclaredExceptionServerTestResult requestResponseUndeclaredException;
  5: RequestResponseTimeoutServerTestResult requestResponseTimeout;
  6: RequestResponseAnyExceptionServerTestResult requestResponseAnyException;
  7: RequestResponseServerErrorServerTestResult requestResponseServerError;
  8: RequestResponseMetadataServerTestResult requestResponseMetadata;
  9: RequestResponseConcurrentServerTestResult requestResponseConcurrent;
  10: RequestResponseBadContentLengthServerTestResult requestResponseBadContentLength;
  100: StreamBasicServerTestResult streamBasic;
  101: StreamChunkTimeoutServerTestResult streamChunkTimeout;
  102: StreamInitialResponseServerTestResult streamInitialResponse;
  103: StreamCreditTimeoutServerTestResult streamCreditTimeout;
  104: StreamDeclaredExceptionServerTestResult streamDeclaredException;
  105: StreamUndeclaredExceptionServerTestResult streamUndeclaredException;
  106: StreamInitialDeclaredExceptionServerTestResult streamInitialDeclaredException;
  107: StreamInitialUndeclaredExceptionServerTestResult streamInitialUndeclaredException;
  108: StreamInitialTimeoutServerTestResult streamInitialTimeout;
  109: StreamCancelServerTestResult streamCancel;
  110: StreamServerErrorServerTestResult streamServerError;
  200: SinkBasicServerTestResult sinkBasic;
  201: SinkChunkTimeoutServerTestResult sinkChunkTimeout;
  202: SinkInitialResponseServerTestResult sinkInitialResponse;
  203: SinkDeclaredExceptionServerTestResult sinkDeclaredException;
  204: SinkUndeclaredExceptionServerTestResult sinkUndeclaredException;
  205: SinkInitialDeclaredExceptionServerTestResult sinkInitialDeclaredException;
  206: SinkServerDeclaredExceptionServerTestResult sinkServerDeclaredException;
  207: SinkAbortServerTestResult sinkAbort;
  300: InteractionConstructorServerTestResult interactionConstructor;
  301: InteractionFactoryFunctionServerTestResult interactionFactoryFunction;
  302: InteractionPersistsStateServerTestResult interactionPersistsState;
  303: InteractionTerminationServerTestResult interactionTermination;
  400: BidiBasicServerTestResult bidiBasic;
  401: BidiInitialResponseServerTestResult bidiInitialResponse;
  402: BidiStreamDeclaredExceptionServerTestResult bidiStreamDeclaredException;
  403: BidiStreamUndeclaredExceptionServerTestResult bidiStreamUndeclaredException;
  404: BidiSinkDeclaredExceptionServerTestResult bidiSinkDeclaredException;
  405: BidiSinkUndeclaredExceptionServerTestResult bidiSinkUndeclaredException;
  406: BidiMethodDeclaredExceptionServerTestResult bidiMethodDeclaredException;
  407: BidiMethodUndeclaredExceptionServerTestResult bidiMethodUndeclaredException;
  408: BidiAbortServerTestResult bidiAbort;
  500: ClosingFrameServerTestResult closingFrame;
}

union ClientTestResult {
  1: RequestResponseBasicClientTestResult requestResponseBasic;
  2: RequestResponseDeclaredExceptionClientTestResult requestResponseDeclaredException;
  3: RequestResponseNoArgVoidResponseClientTestResult requestResponseNoArgVoidResponse;
  4: RequestResponseUndeclaredExceptionClientTestResult requestResponseUndeclaredException;
  5: RequestResponseTimeoutClientTestResult requestResponseTimeout;
  6: RequestResponseAnyExceptionClientTestResult requestResponseAnyException;
  7: RequestResponseServerErrorClientTestResult requestResponseServerError;
  8: RequestResponseMetadataClientTestResult requestResponseMetadata;
  9: RequestResponseConcurrentClientTestResult requestResponseConcurrent;
  10: RequestResponseBadContentLengthClientTestResult requestResponseBadContentLength;
  100: StreamBasicClientTestResult streamBasic;
  101: StreamChunkTimeoutClientTestResult streamChunkTimeout;
  102: StreamInitialResponseClientTestResult streamInitialResponse;
  103: StreamCreditTimeoutClientTestResult streamCreditTimeout;
  104: StreamDeclaredExceptionClientTestResult streamDeclaredException;
  105: StreamUndeclaredExceptionClientTestResult streamUndeclaredException;
  106: StreamInitialDeclaredExceptionClientTestResult streamInitialDeclaredException;
  107: StreamInitialUndeclaredExceptionClientTestResult streamInitialUndeclaredException;
  108: StreamInitialTimeoutClientTestResult streamInitialTimeout;
  109: StreamCancelClientTestResult streamCancel;
  110: StreamServerErrorClientTestResult streamServerError;
  200: SinkBasicClientTestResult sinkBasic;
  201: SinkChunkTimeoutClientTestResult sinkChunkTimeout;
  202: SinkInitialResponseClientTestResult sinkInitialResponse;
  203: SinkDeclaredExceptionClientTestResult sinkDeclaredException;
  204: SinkUndeclaredExceptionClientTestResult sinkUndeclaredException;
  205: SinkInitialDeclaredExceptionClientTestResult sinkInitialDeclaredException;
  206: SinkServerDeclaredExceptionClientTestResult sinkServerDeclaredException;
  207: SinkAbortClientTestResult sinkAbort;
  208: SinkServerErrorClientTestResult sinkServerError;
  300: InteractionConstructorClientTestResult interactionConstructor;
  301: InteractionFactoryFunctionClientTestResult interactionFactoryFunction;
  302: InteractionPersistsStateClientTestResult interactionPersistsState;
  303: InteractionTerminationClientTestResult interactionTermination;
  400: BidiBasicClientTestResult bidiBasic;
  401: BidiInitialResponseClientTestResult bidiInitialResponse;
  402: BidiStreamDeclaredExceptionClientTestResult bidiStreamDeclaredException;
  403: BidiStreamUndeclaredExceptionClientTestResult bidiStreamUndeclaredException;
  404: BidiSinkDeclaredExceptionClientTestResult bidiSinkDeclaredException;
  405: BidiSinkUndeclaredExceptionClientTestResult bidiSinkUndeclaredException;
  406: BidiMethodDeclaredExceptionClientTestResult bidiMethodDeclaredException;
  407: BidiMethodUndeclaredExceptionClientTestResult bidiMethodUndeclaredException;
  408: BidiAbortClientTestResult bidiAbort;
  409: BidiServerErrorClientTestResult bidiServerError;
  500: ClosingFrameClientTestResult closingFrame;
}

struct RequestResponseBasicServerTestResult {
  1: Request request;
  2: optional ChecksumKind checksum;
}

struct RequestResponseDeclaredExceptionServerTestResult {
  1: Request request;
}

struct RequestResponseUndeclaredExceptionServerTestResult {
  1: Request request;
}

struct RequestResponseNoArgVoidResponseServerTestResult {}

struct RequestResponseTimeoutServerTestResult {
  1: Request request;
}

struct RequestResponseAnyExceptionServerTestResult {
  1: Request request;
}

struct RequestResponseServerErrorServerTestResult {
  1: Request request;
}

struct RequestResponseMetadataServerTestResult {
  1: Request request;
  2: map<string, string> headers;
  3: optional i64 clientTimeoutMs;
  4: optional i64 queueTimeoutMs;
  5: optional i32 priority;
}

struct RequestResponseConcurrentServerTestResult {
  1: list<Request> requests;
  2: bool allInFlight;
}

struct RequestResponseBadContentLengthServerTestResult {
  1: Request request;
}

struct StreamBasicServerTestResult {
  1: Request request;
  2: optional ChecksumKind checksum;
}

struct StreamChunkTimeoutServerTestResult {
  1: Request request;
}

struct StreamInitialResponseServerTestResult {
  1: Request request;
}

struct StreamCreditTimeoutServerTestResult {
  1: Request request;
}

struct StreamDeclaredExceptionServerTestResult {
  1: Request request;
}

struct StreamUndeclaredExceptionServerTestResult {
  1: Request request;
}

struct StreamInitialDeclaredExceptionServerTestResult {
  1: Request request;
}

struct StreamInitialUndeclaredExceptionServerTestResult {
  1: Request request;
}

struct StreamInitialTimeoutServerTestResult {
  1: Request request;
}

struct StreamCancelServerTestResult {
  1: Request request;
  2: bool cancelled;
}

struct StreamServerErrorServerTestResult {
  1: Request request;
}

struct SinkBasicServerTestResult {
  1: Request request;
  2: list<Request> sinkPayloads;
  3: optional ChecksumKind checksum;
}

struct SinkChunkTimeoutServerTestResult {
  1: Request request;
  2: list<Request> sinkPayloads;
  3: bool chunkTimeoutException;
}

struct SinkInitialResponseServerTestResult {
  1: Request request;
  2: list<Request> sinkPayloads;
}

struct SinkDeclaredExceptionServerTestResult {
  1: Request request;
  2: optional UserException userException;
}

struct SinkUndeclaredExceptionServerTestResult {
  1: Request request;
  2: optional string exceptionMessage;
}

struct SinkInitialDeclaredExceptionServerTestResult {
  1: Request request;
  2: optional string exceptionMessage;
}

struct SinkServerDeclaredExceptionServerTestResult {
  1: Request request;
  2: optional string exceptionMessage;
}

struct SinkAbortServerTestResult {
  1: Request request;
  2: bool aborted;
}

struct InteractionConstructorServerTestResult {
  1: bool constructorCalled;
}

struct InteractionFactoryFunctionServerTestResult {
  1: i32 initialSum;
}

struct InteractionPersistsStateServerTestResult {}

struct InteractionTerminationServerTestResult {
  1: bool terminationReceived;
}

struct BidiBasicServerTestResult {
  1: Request request;
  2: list<Request> sinkPayloads;
  3: optional ChecksumKind checksum;
}

struct BidiInitialResponseServerTestResult {
  1: Request request;
  2: list<Request> sinkPayloads;
}

struct BidiStreamDeclaredExceptionServerTestResult {
  1: Request request;
  2: list<Request> sinkPayloads;
}

struct BidiStreamUndeclaredExceptionServerTestResult {
  1: Request request;
  2: list<Request> sinkPayloads;
}

struct BidiSinkDeclaredExceptionServerTestResult {
  1: Request request;
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  2: optional UserException userException;
  3: list<Request> sinkPayloads;
}

struct BidiSinkUndeclaredExceptionServerTestResult {
  1: Request request;
  2: optional string exceptionMessage;
  3: list<Request> sinkPayloads;
}

struct BidiMethodDeclaredExceptionServerTestResult {
  1: Request request;
}

struct BidiMethodUndeclaredExceptionServerTestResult {
  1: Request request;
}

struct BidiAbortServerTestResult {
  1: Request request;
  2: bool sinkAborted;
}

struct ClosingFrameServerTestResult {
  1: list<ClientFrame> frames;
}

struct ClientFrame {
  1: i32 type;
  2: optional i32 errorCode;
  3: optional binary errorData;
}

struct RequestResponseBasicClientTestResult {
  1: Response response;
}

struct RequestResponseNoArgVoidResponseClientTestResult {}

struct RequestResponseDeclaredExceptionClientTestResult {
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  1: optional UserException userException;
}

struct RequestResponseUndeclaredExceptionClientTestResult {
  1: string exceptionMessage;
}

struct RequestResponseTimeoutClientTestResult {
  1: bool timeoutException;
}

struct RequestResponseAnyExceptionClientTestResult {
  1: string exceptionMessage;
  2: string typeUri;
  @thrift.Box
  3: optional UserException userException;
}

struct RequestResponseServerErrorClientTestResult {
  1: i32 exceptionType;
  2: string exceptionMessage;
}

struct RequestResponseMetadataClientTestResult {
  1: Response response;
}

struct RequestResponseConcurrentClientTestResult {
  1: list<Response> responses;
}

struct RequestResponseBadContentLengthClientTestResult {
  1: optional i32 transportExceptionType;
}

struct StreamBasicClientTestResult {
  1: list<Response> streamPayloads;
}

struct StreamChunkTimeoutClientTestResult {
  1: list<Response> streamPayloads;
  2: bool chunkTimeoutException;
}

struct StreamInitialResponseClientTestResult {
  1: list<Response> streamPayloads;
  2: Response initialResponse;
}

struct StreamCreditTimeoutClientTestResult {
  1: bool creditTimeoutException;
}

struct StreamDeclaredExceptionClientTestResult {
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  1: optional UserException userException;
}

struct StreamUndeclaredExceptionClientTestResult {
  1: string exceptionMessage;
}

struct StreamInitialDeclaredExceptionClientTestResult {
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  1: optional UserException userException;
}

struct StreamInitialUndeclaredExceptionClientTestResult {
  1: string exceptionMessage;
}

struct StreamInitialTimeoutClientTestResult {
  1: bool timeoutException;
}

struct StreamCancelClientTestResult {
  1: list<Response> streamPayloads;
}

struct StreamServerErrorClientTestResult {
  1: i32 exceptionType;
  2: string exceptionMessage;
}

struct SinkBasicClientTestResult {
  1: Response finalResponse;
}

struct SinkChunkTimeoutClientTestResult {
  1: bool chunkTimeoutException;
}

struct SinkInitialResponseClientTestResult {
  1: Response initialResponse;
  2: Response finalResponse;
}

struct SinkDeclaredExceptionClientTestResult {
  1: bool sinkThrew;
}

struct SinkUndeclaredExceptionClientTestResult {
  1: bool sinkThrew;
}

struct SinkInitialDeclaredExceptionClientTestResult {
  1: bool sinkThrew;
}

struct SinkServerDeclaredExceptionClientTestResult {
  1: bool sinkThrew;
}

struct SinkAbortClientTestResult {}

struct SinkServerErrorClientTestResult {
  1: i32 exceptionType;
  2: string exceptionMessage;
}

struct InteractionConstructorClientTestResult {}

struct InteractionFactoryFunctionClientTestResult {}

struct InteractionPersistsStateClientTestResult {
  1: list<i32> responses;
}

struct InteractionTerminationClientTestResult {}

struct BidiBasicClientTestResult {
  1: list<Response> streamPayloads;
}

struct BidiInitialResponseClientTestResult {
  1: list<Response> streamPayloads;
  2: Response initialResponse;
}

struct BidiStreamDeclaredExceptionClientTestResult {
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  1: optional UserException userException;
  2: list<Response> streamPayloads;
}

struct BidiStreamUndeclaredExceptionClientTestResult {
  1: string exceptionMessage;
  2: list<Response> streamPayloads;
}

struct BidiSinkDeclaredExceptionClientTestResult {
  1: list<Response> streamPayloads;
  2: bool sinkThrew;
}

struct BidiSinkUndeclaredExceptionClientTestResult {
  1: list<Response> streamPayloads;
  2: bool sinkThrew;
}

struct BidiMethodDeclaredExceptionClientTestResult {
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  1: optional UserException userException;
}

struct BidiMethodUndeclaredExceptionClientTestResult {
  1: string exceptionMessage;
}

struct BidiAbortClientTestResult {}

struct BidiServerErrorClientTestResult {
  1: i32 exceptionType;
  2: string exceptionMessage;
}

struct ClosingFrameClientTestResult {}

union ClientInstruction {
  1: RequestResponseBasicClientInstruction requestResponseBasic;
  2: RequestResponseDeclaredExceptionClientInstruction requestResponseDeclaredException;
  3: RequestResponseNoArgVoidResponseClientInstruction requestResponseNoArgVoidResponse;
  4: RequestResponseUndeclaredExceptionClientInstruction requestResponseUndeclaredException;
  5: RequestResponseTimeoutClientInstruction requestResponseTimeout;
  6: RequestResponseAnyExceptionClientInstruction requestResponseAnyException;
  7: RequestResponseServerErrorClientInstruction requestResponseServerError;
  8: RequestResponseMetadataClientInstruction requestResponseMetadata;
  9: RequestResponseConcurrentClientInstruction requestResponseConcurrent;
  10: RequestResponseBadContentLengthClientInstruction requestResponseBadContentLength;
  100: StreamBasicClientInstruction streamBasic;
  101: StreamChunkTimeoutClientInstruction streamChunkTimeout;
  102: StreamInitialResponseClientInstruction streamInitialResponse;
  103: StreamCreditTimeoutClientInstruction streamCreditTimeout;
  104: StreamDeclaredExceptionClientInstruction streamDeclaredException;
  105: StreamUndeclaredExceptionClientInstruction streamUndeclaredException;
  106: StreamInitialDeclaredExceptionClientInstruction streamInitialDeclaredException;
  107: StreamInitialUndeclaredExceptionClientInstruction streamInitialUndeclaredException;
  108: StreamInitialTimeoutClientInstruction streamInitialTimeout;
  109: StreamCancelClientInstruction streamCancel;
  110: StreamServerErrorClientInstruction streamServerError;
  200: SinkBasicClientInstruction sinkBasic;
  201: SinkChunkTimeoutClientInstruction sinkChunkTimeout;
  202: SinkInitialResponseClientInstruction sinkInitialResponse;
  203: SinkDeclaredExceptionClientInstruction sinkDeclaredException;
  204: SinkUndeclaredExceptionClientInstruction sinkUndeclaredException;
  205: SinkInitialDeclaredExceptionClientInstruction sinkInitialDeclaredException;
  206: SinkServerDeclaredExceptionClientInstruction sinkServerDeclaredException;
  207: SinkAbortClientInstruction sinkAbort;
  208: SinkServerErrorClientInstruction sinkServerError;
  300: InteractionConstructorClientInstruction interactionConstructor;
  301: InteractionFactoryFunctionClientInstruction interactionFactoryFunction;
  302: InteractionPersistsStateClientInstruction interactionPersistsState;
  303: InteractionTerminationClientInstruction interactionTermination;
  400: BidiBasicClientInstruction bidiBasic;
  401: BidiInitialResponseClientInstruction bidiInitialResponse;
  402: BidiStreamDeclaredExceptionClientInstruction bidiStreamDeclaredException;
  403: BidiStreamUndeclaredExceptionClientInstruction bidiStreamUndeclaredException;
  404: BidiSinkDeclaredExceptionClientInstruction bidiSinkDeclaredException;
  405: BidiSinkUndeclaredExceptionClientInstruction bidiSinkUndeclaredException;
  406: BidiMethodDeclaredExceptionClientInstruction bidiMethodDeclaredException;
  407: BidiMethodUndeclaredExceptionClientInstruction bidiMethodUndeclaredException;
  408: BidiAbortClientInstruction bidiAbort;
  409: BidiServerErrorClientInstruction bidiServerError;
  500: ClosingFrameClientInstruction closingFrame;
}

union ServerInstruction {
  1: RequestResponseBasicServerInstruction requestResponseBasic;
  2: RequestResponseDeclaredExceptionServerInstruction requestResponseDeclaredException;
  3: RequestResponseNoArgVoidResponseServerInstruction requestResponseNoArgVoidResponse;
  4: RequestResponseUndeclaredExceptionServerInstruction requestResponseUndeclaredException;
  5: RequestResponseTimeoutServerInstruction requestResponseTimeout;
  6: RequestResponseAnyExceptionServerInstruction requestResponseAnyException;
  7: RequestResponseServerErrorServerInstruction requestResponseServerError;
  8: RequestResponseMetadataServerInstruction requestResponseMetadata;
  9: RequestResponseConcurrentServerInstruction requestResponseConcurrent;
  10: RequestResponseBadContentLengthServerInstruction requestResponseBadContentLength;
  100: StreamBasicServerInstruction streamBasic;
  101: StreamChunkTimeoutServerInstruction streamChunkTimeout;
  102: StreamInitialResponseServerInstruction streamInitialResponse;
  103: StreamCreditTimeoutServerInstruction streamCreditTimeout;
  104: StreamDeclaredExceptionServerInstruction streamDeclaredException;
  105: StreamUndeclaredExceptionServerInstruction streamUndeclaredException;
  106: StreamInitialDeclaredExceptionServerInstruction streamInitialDeclaredException;
  107: StreamInitialUndeclaredExceptionServerInstruction streamInitialUndeclaredException;
  108: StreamInitialTimeoutServerInstruction streamInitialTimeout;
  109: StreamCancelServerInstruction streamCancel;
  110: StreamServerErrorServerInstruction streamServerError;
  200: SinkBasicServerInstruction sinkBasic;
  201: SinkChunkTimeoutServerInstruction sinkChunkTimeout;
  202: SinkInitialResponseServerInstruction sinkInitialResponse;
  203: SinkDeclaredExceptionServerInstruction sinkDeclaredException;
  204: SinkUndeclaredExceptionServerInstruction sinkUndeclaredException;
  205: SinkInitialDeclaredExceptionServerInstruction sinkInitialDeclaredException;
  206: SinkServerDeclaredExceptionServerInstruction sinkServerDeclaredException;
  207: SinkAbortServerInstruction sinkAbort;
  208: SinkServerErrorServerInstruction sinkServerError;
  300: InteractionConstructorServerInstruction interactionConstructor;
  301: InteractionFactoryFunctionServerInstruction interactionFactoryFunction;
  302: InteractionPersistsStateServerInstruction interactionPersistsState;
  303: InteractionTerminationServerInstruction interactionTermination;
  400: BidiBasicServerInstruction bidiBasic;
  401: BidiInitialResponseServerInstruction bidiInitialResponse;
  402: BidiStreamDeclaredExceptionServerInstruction bidiStreamDeclaredException;
  403: BidiStreamUndeclaredExceptionServerInstruction bidiStreamUndeclaredException;
  404: BidiSinkDeclaredExceptionServerInstruction bidiSinkDeclaredException;
  405: BidiSinkUndeclaredExceptionServerInstruction bidiSinkUndeclaredException;
  406: BidiMethodDeclaredExceptionServerInstruction bidiMethodDeclaredException;
  407: BidiMethodUndeclaredExceptionServerInstruction bidiMethodUndeclaredException;
  408: BidiAbortServerInstruction bidiAbort;
  409: BidiServerErrorServerInstruction bidiServerError;
  500: ClosingFrameServerInstruction closingFrame;
}

struct RequestResponseBasicClientInstruction {
  1: Request request;
  2: optional ChecksumKind checksum;
}

struct RequestResponseDeclaredExceptionClientInstruction {
  1: Request request;
}

struct RequestResponseUndeclaredExceptionClientInstruction {
  1: Request request;
}

struct RequestResponseNoArgVoidResponseClientInstruction {}

struct RequestResponseTimeoutClientInstruction {
  1: Request request;
  2: i64 timeoutMs;
}

struct RequestResponseAnyExceptionClientInstruction {
  1: Request request;
}

struct RequestResponseServerErrorClientInstruction {
  1: Request request;
  2: bool unknownMethod;
}

struct RequestResponseMetadataClientInstruction {
  1: Request request;
  2: map<string, string> writeHeaders;
  3: map<string, string> persistentHeaders;
  4: optional i64 timeoutMs;
  5: optional i64 queueTimeoutMs;
  6: bool clientOnlyTimeouts;
  7: optional i32 priority;
}

struct RequestResponseConcurrentClientInstruction {
  1: list<Request> requests;
}

struct RequestResponseBadContentLengthClientInstruction {
  1: Request request;
}

struct StreamBasicClientInstruction {
  1: Request request;
  2: i64 bufferSize;
  3: optional StreamMemoryBufferSize memoryBufferSize;
  4: optional ChecksumKind checksum;
}

struct StreamMemoryBufferSize {
  1: i64 targetBytes;
  2: i32 initialChunks;
  3: i32 maxChunks;
}

struct StreamChunkTimeoutClientInstruction {
  1: Request request;
  2: i64 chunkTimeoutMs;
}

struct StreamInitialResponseClientInstruction {
  1: Request request;
}

struct StreamCreditTimeoutClientInstruction {
  1: Request request;
  2: i64 creditTimeoutMs;
}

struct StreamDeclaredExceptionClientInstruction {
  1: Request request;
}

struct StreamUndeclaredExceptionClientInstruction {
  1: Request request;
}

struct StreamInitialDeclaredExceptionClientInstruction {
  1: Request request;
}

struct StreamInitialUndeclaredExceptionClientInstruction {
  1: Request request;
}

struct StreamInitialTimeoutClientInstruction {
  1: Request request;
  2: i64 timeoutMs;
}

struct StreamCancelClientInstruction {
  1: Request request;
  2: i64 consumeCount;
  3: optional i64 giveUpAfterMs;
  4: optional i64 dropUnreadAfterMs;
}

struct StreamServerErrorClientInstruction {
  1: Request request;
}

struct SinkBasicClientInstruction {
  1: Request request;
  2: list<Request> sinkPayloads;
  3: optional ChecksumKind checksum;
}

struct SinkChunkTimeoutClientInstruction {
  1: Request request;
  2: list<Request> sinkPayloads;
  3: i64 chunkTimeoutMs;
}

struct SinkInitialResponseClientInstruction {
  1: Request request;
  2: list<Request> sinkPayloads;
}

struct SinkDeclaredExceptionClientInstruction {
  1: Request request;
  2: optional UserException userException;
}

struct SinkUndeclaredExceptionClientInstruction {
  1: Request request;
  2: optional string exceptionMessage;
}

struct SinkInitialDeclaredExceptionClientInstruction {
  1: Request request;
  2: optional string exceptionMessage;
}

struct SinkServerDeclaredExceptionClientInstruction {
  1: Request request;
  2: optional string exceptionMessage;
}

struct SinkAbortClientInstruction {
  1: Request request;
  2: optional i64 giveUpAfterMs;
  3: optional i64 dropUnreadAfterMs;
}

struct SinkServerErrorClientInstruction {
  1: Request request;
}

struct InteractionConstructorClientInstruction {}

struct InteractionFactoryFunctionClientInstruction {
  1: i32 initialSum;
}

struct InteractionPersistsStateClientInstruction {
  1: optional i32 initialSum;
  2: list<i32> valuesToAdd;
}

struct InteractionTerminationClientInstruction {
  1: optional i32 initialSum;
}

struct BidiBasicClientInstruction {
  1: Request request;
  2: list<Request> sinkPayloads;
  3: optional ChecksumKind checksum;
}

struct BidiInitialResponseClientInstruction {
  1: Request request;
  2: list<Request> sinkPayloads;
}

struct BidiStreamDeclaredExceptionClientInstruction {
  1: Request request;
  2: list<Request> sinkPayloads;
}

struct BidiStreamUndeclaredExceptionClientInstruction {
  1: Request request;
  2: list<Request> sinkPayloads;
}

struct BidiSinkDeclaredExceptionClientInstruction {
  1: Request request;
  2: list<Request> sinkPayloads;
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  3: optional UserException userException;
}

struct BidiSinkUndeclaredExceptionClientInstruction {
  1: Request request;
  2: list<Request> sinkPayloads;
  3: optional string exceptionMessage;
}

struct BidiMethodDeclaredExceptionClientInstruction {
  1: Request request;
}

struct BidiMethodUndeclaredExceptionClientInstruction {
  1: Request request;
}

struct BidiAbortClientInstruction {
  1: Request request;
  2: optional i64 giveUpAfterMs;
  3: optional i64 dropUnreadAfterMs;
}

struct BidiServerErrorClientInstruction {
  1: Request request;
}

enum ChecksumKind {
  NONE = 0,
  CRC32 = 1,
  XXH3_64 = 2,
}

enum CallKind {
  REQUEST_RESPONSE = 0,
  STREAM = 1,
  SINK = 2,
  BIDI = 3,
}

struct ClosingFrameClientInstruction {
  1: CallKind call;
  2: Request request;
}

struct RequestResponseBasicServerInstruction {
  1: Response response;
}

struct RequestResponseDeclaredExceptionServerInstruction {
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  1: optional UserException userException;
}

struct RequestResponseUndeclaredExceptionServerInstruction {
  1: string exceptionMessage;
}

struct RequestResponseNoArgVoidResponseServerInstruction {}

struct RequestResponseTimeoutServerInstruction {
  1: i64 timeoutMs;
}

struct RequestResponseAnyExceptionServerInstruction {
  1: string exceptionMessage;
  2: string typeUri;
  @thrift.Box
  3: optional UserException userException;
}

union ServerError {
  1: string loadshedding;
  2: TaskExpiredServerError taskExpired;
  3: ResponseTooBigServerError responseTooBig;
  4: string undeclaredException;
  5: ErrorFrameServerError errorFrame;
  6: binary firstResponseMetadata;
  7: binary chunkMetadata;
}

struct TaskExpiredServerError {
  1: i64 taskExpireTimeMs;
  2: i64 handlerTimeMs;
}

struct ResponseTooBigServerError {
  1: i64 maxResponseSize;
  2: i64 responseSize;
}

struct ErrorFrameServerError {
  1: i32 code;
  2: string message;
}

struct RequestResponseServerErrorServerInstruction {
  1: ServerError error;
}

struct RequestResponseMetadataServerInstruction {
  1: Response response;
}

struct RequestResponseConcurrentServerInstruction {
  1: i64 requestCount;
}

struct RequestResponseBadContentLengthServerInstruction {
  1: Response response;
  2: i64 contentLength;
}

struct StreamBasicServerInstruction {
  1: list<Response> streamPayloads;
}

struct StreamChunkTimeoutServerInstruction {
  1: list<Response> streamPayloads;
  2: i64 chunkTimeoutMs;
}

struct StreamInitialResponseServerInstruction {
  1: list<Response> streamPayloads;
  2: Response initialResponse;
}

struct StreamCreditTimeoutServerInstruction {
  1: list<Response> streamPayloads;
  2: i64 streamExpireTime;
}

struct StreamDeclaredExceptionServerInstruction {
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  1: optional UserException userException;
}

struct StreamUndeclaredExceptionServerInstruction {
  1: string exceptionMessage;
}

struct StreamInitialDeclaredExceptionServerInstruction {
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  1: optional UserException userException;
}

struct StreamInitialUndeclaredExceptionServerInstruction {
  1: string exceptionMessage;
}

struct StreamInitialTimeoutServerInstruction {
  1: i64 timeoutMs;
}

struct StreamCancelServerInstruction {
  1: optional i64 initialResponseDelayMs;
}

struct StreamServerErrorServerInstruction {
  1: ServerError error;
}

struct SinkBasicServerInstruction {
  1: Response finalResponse;
  2: i64 bufferSize;
  3: optional i64 consumeCount;
}

struct SinkChunkTimeoutServerInstruction {
  1: Response finalResponse;
  2: i64 chunkTimeoutMs;
}

struct SinkInitialResponseServerInstruction {
  1: Response initialResponse;
  2: Response finalResponse;
  3: i64 bufferSize;
}

struct SinkDeclaredExceptionServerInstruction {
  1: i64 bufferSize;
}

struct SinkUndeclaredExceptionServerInstruction {
  1: i64 bufferSize;
}

struct SinkInitialDeclaredExceptionServerInstruction {
  1: i64 bufferSize;
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  2: optional UserException userException;
}

struct SinkServerDeclaredExceptionServerInstruction {
  1: i64 bufferSize;
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  2: optional UserException userException;
}

struct SinkAbortServerInstruction {
  1: optional i64 initialResponseDelayMs;
}

struct SinkServerErrorServerInstruction {
  1: ServerError error;
}

struct InteractionConstructorServerInstruction {}

struct InteractionFactoryFunctionServerInstruction {}

struct InteractionPersistsStateServerInstruction {}

struct InteractionTerminationServerInstruction {}

struct BidiBasicServerInstruction {
  1: list<Response> streamPayloads;
}

struct BidiInitialResponseServerInstruction {
  1: list<Response> streamPayloads;
  2: Response initialResponse;
}

struct BidiStreamDeclaredExceptionServerInstruction {
  1: list<Response> streamPayloads;
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  2: optional UserException userException;
}

struct BidiStreamUndeclaredExceptionServerInstruction {
  1: list<Response> streamPayloads;
  2: string exceptionMessage;
}

struct BidiSinkDeclaredExceptionServerInstruction {
  1: list<Response> streamPayloads;
}

struct BidiSinkUndeclaredExceptionServerInstruction {
  1: list<Response> streamPayloads;
}

struct BidiMethodDeclaredExceptionServerInstruction {
  // TODO(dokwon): Remove @thrift.Box after fixing incomplete type bug.
  @thrift.Box
  1: optional UserException userException;
}

struct BidiMethodUndeclaredExceptionServerInstruction {
  1: string exceptionMessage;
}

struct BidiAbortServerInstruction {
  1: optional i64 initialResponseDelayMs;
}

struct BidiServerErrorServerInstruction {
  1: ServerError error;
}

struct ClosingFrameServerInstruction {
  1: CallKind call;
  2: FirstResponse firstResponse;
}

union FirstResponse {
  1: ErrorFrameServerError errorFrame;
  2: binary metadata;
  3: binary chunkMetadata;
}

interaction BasicInteraction {
  void init();
  // adds i to the cumulative sum and returns the new value
  i32 add(1: i32 i);
}

service RPCConformanceService {
  // =================== Conformance framework - Only for Server Tests ===================
  void sendTestCase(1: RpcTestCase testCase);
  ServerTestResult getTestResult();

  // =================== Conformance framework - Only for Client Tests ===================
  RpcTestCase getTestCase();
  void sendTestResult(1: ClientTestResult result);

  // =================== Request-Response ===================
  Response requestResponseBasic(1: Request req);
  void requestResponseDeclaredException(1: Request req) throws (
    1: UserException e,
  );
  void requestResponseUndeclaredException(1: Request req);
  void requestResponseNoArgVoidResponse();
  Response requestResponseTimeout(1: Request req);

  // =================== Stream ===================
  stream<Response> streamBasic(1: Request req);
  stream<Response> streamChunkTimeout(1: Request req);
  Response, stream<Response> streamInitialResponse(1: Request req);
  stream<Response> streamCreditTimeout(1: Request req);
  stream<Response throws (1: UserException e)> streamDeclaredException(
    1: Request req,
  );
  stream<Response> streamUndeclaredException(1: Request req);
  stream<Response> streamInitialDeclaredException(1: Request req) throws (
    1: UserException e,
  );
  stream<Response> streamInitialUndeclaredException(1: Request req);
  stream<Response> streamInitialTimeout(1: Request req);

  // =================== Sink ===================
  sink<Request, Response> sinkBasic(1: Request req);
  sink<Request, Response> sinkChunkTimeout(1: Request req);
  Response, sink<Request, Response> sinkInitialResponse(1: Request req);
  # Service function may throw an expected exception
  sink<Request, Response> sinkInitialDeclaredException(1: Request req) throws (
    1: UserException e,
  );
  # Sink exception, client side may throw an expected exception and send to server
  sink<Request throws (1: UserException e), Response> sinkDeclaredException(
    1: Request req,
  );
  # Sink exception, server side may throw an expected exception and send to client
  sink<
    Request,
    Response throws (1: UserException e)
  > sinkServerDeclaredException(1: Request req);
  sink<Request, Response> sinkUndeclaredException(1: Request req);

  // =================== Interactions ===================
  performs BasicInteraction;
  BasicInteraction basicInteractionFactoryFunction(-1: i32 initialSum);

  // =================== BiDi Streaming ===================
  sink<Request>, stream<Response> bidiBasic(1: Request req);
  Response, sink<Request>, stream<Response> bidiInitialResponse(1: Request req);
  sink<Request>, stream<
    Response throws (1: UserException e)
  > bidiStreamDeclaredException(1: Request req);
  sink<Request>, stream<Response> bidiStreamUndeclaredException(1: Request req);
  sink<Request throws (1: UserException e)>, stream<
    Response
  > bidiSinkDeclaredException(1: Request req);
  sink<Request>, stream<Response> bidiSinkUndeclaredException(1: Request req);
  sink<Request>, stream<Response> bidiMethodDeclaredException(
    1: Request req,
  ) throws (1: UserException e);
  sink<Request>, stream<Response> bidiMethodUndeclaredException(1: Request req);
}

service RPCStatelessConformanceService {
  // =================== Request-Response ===================
  ServerTestResult requestResponseBasic(1: Request req);
  void requestResponseDeclaredException(
    1: ServerInstruction serverInstr,
  ) throws (1: UserException e);
  void requestResponseUndeclaredException(1: ServerInstruction serverInstr);
  void requestResponseNoArgVoidResponse();

  // =================== Stream ===================
  stream<Response> streamBasic(1: ServerInstruction serverInstr);
  Response, stream<Response> streamInitialResponse(
    1: ServerInstruction serverInstr,
  );
  stream<Response throws (1: UserException e)> streamDeclaredException(
    1: ServerInstruction serverInstr,
  );
  stream<Response> streamUndeclaredException(1: ServerInstruction serverInstr);
  stream<Response> streamInitialDeclaredException(
    1: ServerInstruction serverInstr,
  ) throws (1: UserException e);
  stream<Response> streamInitialUndeclaredException(
    1: ServerInstruction serverInstr,
  );

  // =================== Sink ===================
  sink<Request, Response> sinkBasic(1: ServerInstruction serverInstr);
  sink<Request, Response> sinkChunkTimeout(1: ServerInstruction serverInstr);
  Response, sink<Request, Response> sinkInitialResponse(
    1: ServerInstruction serverInstr,
  );
  # Service function may throw an expected exception
  sink<Request, Response> sinkInitialDeclaredException(
    1: ServerInstruction serverInstr,
  ) throws (1: UserException e);
  # Sink exception, client side may throw an expected exception and send to server
  sink<Request throws (1: UserException e), Response> sinkDeclaredException(
    1: ServerInstruction serverInstr,
  );
  # Sink exception, server side may throw an expected exception and send to client
  sink<
    Request,
    Response throws (1: UserException e)
  > sinkServerDeclaredException(1: ServerInstruction serverInstr);
  sink<Request, Response> sinkUndeclaredException(
    1: ServerInstruction serverInstr,
  );

  // =================== BiDi Streaming ===================
  sink<Request>, stream<Response> bidiBasic(1: ServerInstruction serverInstr);
  Response, sink<Request>, stream<Response> bidiInitialResponse(
    1: ServerInstruction serverInstr,
  );
  sink<Request>, stream<
    Response throws (1: UserException e)
  > bidiStreamDeclaredException(1: ServerInstruction serverInstr);
  sink<Request>, stream<Response> bidiStreamUndeclaredException(
    1: ServerInstruction serverInstr,
  );
  sink<Request throws (1: UserException e)>, stream<
    Response
  > bidiSinkDeclaredException(1: ServerInstruction serverInstr);
  sink<Request>, stream<Response> bidiSinkUndeclaredException(
    1: ServerInstruction serverInstr,
  );
  sink<Request>, stream<Response> bidiMethodDeclaredException(
    1: ServerInstruction serverInstr,
  ) throws (1: UserException e);
  sink<Request>, stream<Response> bidiMethodUndeclaredException(
    1: ServerInstruction serverInstr,
  );
}
