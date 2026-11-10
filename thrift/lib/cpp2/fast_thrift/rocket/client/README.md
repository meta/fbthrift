# FastTransport Rocket Client Handlers

## Overview

This directory contains pipeline handlers for the client-side RSocket
implementation in the **FastTransport** framework. These handlers manage
connection setup and stream state for outbound requests and inbound responses.

---

## Handlers

| Handler | Purpose |
|---------|---------|
| `RocketClientFrameCodecHandler` | Duplex codec: parse IOBuf→ParsedFrame (read), extract serialized frame (write) |
| `RocketClientConnectionErrorHandler` | Intercept connection-level ERROR frames (streamId=0): convert faults to exceptions, relay CONNECTION_CLOSE as event |
| `RocketClientSetupFrameHandler` | Sends the RSocket SETUP frame on connect and refuses writes until it is sent |
| `RocketClientStreamStateHandler` | Manages stream IDs and request/response correlation |
| `RocketClientRequestResponseHandler` | Inbound handler for REQUEST_RESPONSE responses: checks each response and turns a protocol violation into a stream ERROR(INVALID) |

---

## Pipeline Architecture

```
Outbound: App → RocketClientStreamStateHandler → RocketClientSetupFrameHandler → RocketClientFrameCodecHandler → Transport
Inbound:  App ← RocketClientRequestResponseHandler ← RocketClientStreamStateHandler ← RocketClientConnectionErrorHandler ← RocketClientSetupFrameHandler ← RocketClientFrameCodecHandler ← Transport
```

Note: `RocketClientSetupFrameHandler` sends the setup frame on connect and refuses every write
until it is sent. All reads pass through unchanged.

---

# RocketClientSetupFrameHandler

## Overview

The `RocketClientSetupFrameHandler` is a **duplex handler** responsible for
sending the RSocket SETUP frame when a client connection is established.

The setup frame is sent immediately via the `onPipelineActive` event, before any
application requests.

## Key Responsibilities

1. **Setup Frame Serialization**: Serializes and sends the SETUP frame with
   protocol version and connection parameters on connect
2. **Immediate Send**: Sends the SETUP frame immediately when `onPipelineActive` is
   called (correct RSocket semantics)
3. **Factory Support**: Accepts a factory function for dynamic metadata
   creation at connection time
4. **Passthrough**: All reads, and every write after SETUP, pass through
   unchanged
5. **Write Refusal**: Until SETUP is sent, returns `Error` for every write:
   before the first activation, after a failed SETUP write and after
   `onPipelineInactive`, because RSocket requires SETUP as the first frame.
   If the SETUP write returns `Error`, closes the pipeline from an EventBase
   loop callback, after the activation returns

## Protocol Constants

The handler uses the following RSocket protocol constants:

| Constant | Value | Description |
|----------|-------|-----------|
| `kRSocketMajorVersion` | 1 | RSocket protocol major version |
| `kRSocketMinorVersion` | 0 | RSocket protocol minor version |
| `kMaxKeepaliveTime` | 2^31 - 1 | Max keepalive time (client-side keepalive not supported) |
| `kMaxLifetime` | 2^31 - 1 | Max connection lifetime (not enforced) |

Note: Resume and Lease features are not currently supported, matching the
existing Thrift RSocket implementation.

## Usage Example

```cpp
#include <thrift/lib/cpp2/fast_thrift/rocket/client/RocketClientSetupFrameHandler.h>

using namespace apache::thrift::fast_thrift::rocket;

// Create handler with factory function for metadata creation
RocketClientSetupFrameHandler handler(
    // Factory: metadata created at connect time
    [&]() {
      auto metadata = createDynamicMetadata();
      return std::make_pair(std::move(metadata), nullptr);
    });

// Add to pipeline - SETUP frame will be sent when pipeline->activate() is called
// pipeline.addHandler(std::move(handler));
```

## Message Flow

### On Connect (Setup Frame Sent)

```
┌─────────────────────────────────────┐
│ pipeline->activate()                │
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│ RocketClientSetupFrameHandler::onPipelineActive  │
│ 1. Fire connect to next handler     │
│ 2. Call factory to create metadata  │
│ 3. Serialize SETUP frame            │
│ 4. Fire write to send setup frame   │
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────┐
│ SETUP Frame         │
│ (to transport)      │
└─────────────────────┘
```

## Testing

```bash
buck2 test fbcode//thrift/lib/cpp2/fast_thrift/rocket/client/test:client_setup_frame_handler_test
```

---

# Client Stream State Handler

## Overview

The Client Stream State Handler is a pipeline handler for managing client-side
RSocket stream state in the **FastTransport** framework. It sits between the
application layer and the framing layer, handling stream ID generation and
lifecycle management for outbound requests and inbound responses.

---

## Architecture

### Pipeline Position

```
App Layer <-> RocketClientStreamStateHandler <-> FrameHandler <-> Transport
```

The handler bridges the gap between application-level request/response semantics
and the stream-based RSocket protocol, abstracting away stream ID management
from the application.

### Key Responsibilities

1. **Stream ID Generation**: Generates odd-numbered stream IDs (1, 3, 5, ...)
   per RSocket protocol specification for client-initiated streams
2. **Stream State Tracking**: Maintains a mapping of active stream IDs to their
   associated context (e.g., request callbacks)
3. **Response Correlation**: Matches inbound frames to their originating
   requests using stream IDs
4. **Lifecycle Management**: Cleans up stream state when terminal frames are
   received (ERROR, CANCEL, or PAYLOAD with complete flag)

---

## Message Types

### RocketRequestMessage

Outbound request from the application layer (~30 bytes with pragma pack).
Frame-specific parameters are inlined directly into the struct rather than
using a separate TypeErasedBox, allowing the struct to fit within
TypeErasedBox's inline capacity of 56 bytes.

```cpp
#pragma pack(push, 1)
struct RocketRequestMessage {
  std::unique_ptr<folly::IOBuf> metadata;  // 8B - Request metadata
  std::unique_ptr<folly::IOBuf> data;      // 8B - Request data
  uint32_t streamId{kInvalidStreamId};     // 4B - Assigned by StreamStateHandler
  uint32_t requestHandle{kNoRequestHandle}; // 4B - Opaque handle for response correlation
  uint32_t initialRequestN{0};              // 4B - Initial request N for streaming frames
  frame::FrameType frameType;             // 1B - REQUEST_RESPONSE, REQUEST_STREAM, etc.
  bool complete{false};                     // 1B - Complete flag for REQUEST_CHANNEL
};
#pragma pack(pop)
```

### RocketResponseMessage

Inbound response delivered to the application layer (~45 bytes with pragma pack).

```cpp
#pragma pack(push, 1)
struct RocketResponseMessage {
  frame::read::ParsedFrame frame;              // 40B - The parsed inbound frame
  uint32_t requestHandle{kNoRequestHandle}; // 4B - Opaque handle for response correlation
  frame::FrameType requestFrameType;      // 1B - The original request's frame type
};
#pragma pack(pop)
```

---

## Message Flow

### Outbound (Application → Transport)

```
┌───────────────────────┐
│ RocketRequestMessage  │
│ - metadata            │
│ - data                │
│ - streamId            │
│ - requestHandle       │
│ - initialRequestN     │
│ - frameType           │
│ - complete            │
└─────────┬─────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│ RocketClientStreamStateHandler            │
│ 1. Generate streamId (1, 3, 5, ...) │
│ 2. Store streamId → handle mapping  │
│ 3. Assign streamId to message       │
└─────────┬───────────────────────────┘
          │
          ▼
┌───────────────────────┐
│ RocketRequestMessage  │
│ - metadata            │
│ - data                │
│ - streamId (assigned) │
│ - requestHandle       │
│ - initialRequestN     │
│ - frameType           │
│ - complete            │
└───────────────────────┘
          │
          ▼
    [FrameHandler]
```

### Inbound (Transport → Application)

```
    [FrameHandler]
          │
          ▼
┌─────────────────────┐
│ ParsedFrame         │
│ - streamId          │
│ - frameType         │
│ - payload           │
└─────────┬───────────┘
          │
          ▼
┌──────────────────────────────────────────┐
│ RocketClientStreamStateHandler                 │
│ 1. Check if connection frame → pass thru │
│ 2. Check if terminal frame               │
│ 3. Look up handle by streamId            │
│ 4. Remove from active streams            │
└─────────┬────────────────────────────────┘
          │
          ▼
┌───────────────────────┐
│ RocketResponseMessage │
│ - frame               │
│ - requestHandle       │
│ - requestFrameType    │
└───────────────────────┘
          │
          ▼
    [Application]
```

---

## Usage Example

```cpp
#include <thrift/lib/cpp2/fast_thrift/rocket/client/RocketClientStreamStateHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>

using namespace apache::thrift::fast_thrift::rocket;
using channel_pipeline::erase_and_box;

// Create handler
RocketClientStreamStateHandler handler;

// The handler is typically added to a channel pipeline:
// pipeline.addHandler<RocketClientStreamStateHandler>();

// Outbound: Application sends a request
RocketRequestMessage request{
    .metadata = std::move(serializedMetadata),
    .data = std::move(serializedData),
    .requestHandle = myRequestHandle,  // Opaque 4-byte handle for correlation
    .initialRequestN = 0,              // Not used for REQUEST_RESPONSE
    .frameType = frame::FrameType::REQUEST_RESPONSE,
    .complete = false,                 // Not used for REQUEST_RESPONSE
};

// The handler assigns a streamId and stores the streamId → handle mapping
// The message is forwarded to the next handler with streamId assigned

// Inbound: Handler receives RocketResponseMessage and:
// - Passes through connection frames (streamId == 0)
// - Passes through non-terminal frames
// - For terminal frames: looks up handle, removes stream, attaches handle to response
```

---

## Terminal Frame Detection

A frame is considered terminal (ending the stream) if it is one of:

- **ERROR frame**: Stream failed with an error
- **CANCEL frame**: Stream was cancelled
- **PAYLOAD frame with complete flag**: Normal stream completion

Non-terminal frames (e.g., REQUEST_N for flow control) pass through without
affecting stream state.

---

## Error Handling

### Exception Handling Design

Exception handling methods return `void` because exceptions must always be
handled/propagated — they represent terminal failures, not flow control signals:

```cpp
template <typename Context>
void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
  // Fail all active streams with the error
  failAllActiveStreams(ctx, e);
  ctx.fireException(std::move(e));
}
```

### Backpressure Semantics

`Result::Backpressure` is a **soft signal**, not an error:
- The current operation **was accepted** and will be processed
- It is an explicit signal telling the handler to **slow down**
- Handlers **may or may not** choose to respect this signal
- Unlike errors, backpressure does not require terminating or failing the stream

### Connection-Level Errors

When `onException` is called (e.g., transport failure), the handler:

1. Iterates over all active streams
2. Fires a `ClientResponseMessage` with the error for each stream
3. Clears the active streams map
4. Propagates the exception to the next handler

### Unknown Stream ID

If a frame arrives for an unknown stream ID, the handler logs an error and
drops the frame. This can happen if:

- The stream was already terminated
- A protocol error occurred
- The frame is malformed

---

## Testing

Run the unit tests with:

```bash
buck2 test fbcode//thrift/lib/cpp2/fast_thrift/rocket/client/test:client_stream_state_handler_test
```

---

## Code Standards

All code follows:
- Thrift C++ Coding Guidelines
- C++ Core Guidelines naming conventions
