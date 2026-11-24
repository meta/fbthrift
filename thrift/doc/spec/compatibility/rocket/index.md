---
description: Rocket client behavior as implemented beyond the Rocket protocol
---

# Rocket

## Request-Response

A client asked for a checksum sends a [`Checksum`][RpcMetadata.thrift] of the request's data in the
`checksum` field of its `RequestRpcMetadata` using the declared algorithm (currently `CRC32` or
`XXH3_64`).

[RpcMetadata.thrift]: https://github.com/facebook/fbthrift/blob/main/thrift/lib/thrift/RpcMetadata.thrift

### Internal Server Error

A server rejects a request with an [ERROR frame][error frame] whose code is `REJECTED`, `CANCELED`,
or `INVALID`. If the first response is an ERROR frame with any other [code][error codes], the client
ignores the frame's data and raises a `TApplicationException` of type `UNKNOWN` with the message
`Unexpected error frame type: <code>`, e.g. `Unexpected error frame type: 513` if it received an
`APPLICATION_ERROR` error code in the ERROR frame. The client then sends [CANCEL][cancel frame] for
a stream **and then**, for a sink or bidi stream, an ERROR frame with code `CANCELED` and no data.
The client sends nothing for a request-response call.

[error codes]: https://rsocket.io/about/protocol/#error-codes
[error frame]: https://rsocket.io/about/protocol/#error-frame-0x0b
[cancel frame]: https://rsocket.io/about/protocol/#cancel-frame-0x09

## Stream

A client asked for a checksum sends one with a stream request, as in
[Request-Response](#request-response).

### Stream Initial Response

When the client can't accept the first response of a call, it closes the call with these frames. A
request-response call sends nothing. ERROR frames with code `CANCELED` carry no data.

| First response | Stream | Sink | Bidi |
| --- | --- | --- | --- |
| Metadata that doesn't decode | CANCEL | ERROR `CANCELED` | ERROR `CANCELED` |
| `PayloadMetadata` with no member set | CANCEL | ERROR `APPLICATION_ERROR` with data `apache::thrift::TApplicationException: process first response error` | CANCEL, then the sink's ERROR |
| An undeclared exception | CANCEL | ERROR `APPLICATION_ERROR` with data `apache::thrift::TApplicationException: never called sink object` | The sink's ERROR, then CANCEL |
| A checksum that doesn't match its data | CANCEL | ERROR `CANCELED` | ERROR `CANCELED` |

A client that verifies checksums raises a `TApplicationException` of type `CHECKSUM_MISMATCH` with
the message `Checksum mismatch` when the checksum in a first response's `ResponseRpcMetadata`
doesn't match its data.

### Stream Responses

A client that verifies checksums fails a stream/bidi stream with a `TApplicationException` of type
`CHECKSUM_MISMATCH` and the message `Checksum mismatch` when the checksum in a chunk's
`StreamPayloadMetadata` doesn't match its data.

## Sink

A client asked for a checksum sends one with a sink request, as in
[Request-Response](#request-response).

### Sink Final Response

A client that verifies checksums raises a `TApplicationException` of type `CHECKSUM_MISMATCH` with
the message `Checksum mismatch` when the checksum in a final response's `StreamPayloadMetadata`
doesn't match its data.

## Bidi

A client asked for a checksum sends one with a bidi request, as in
[Request-Response](#request-response).
