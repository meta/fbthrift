# Fast Thrift client admission control

Status: discussion draft. This document records the design conversation; it
does not authorize or include a runtime implementation.

## Problem

The uCache MetaRoCE route currently protects mcrouter with a route-specific,
hard-coded cap of 32 live requests. The cap is checked before the generated
`mcGet()` call, so a rejected request avoids Thrift serialization. It prevents
unbounded request fan-out, but it has no queue, is specific to one mcrouter
route, and does not protect other fast_thrift clients.

Admission control belongs in the fast_thrift client layer, above transport
selection. TCP, MetaRoCE, XSK, and future transports should see only requests
that the client has admitted.

The desired policy for request-response RPCs is:

1. Allow at most 32 requests to be live in the connection pipeline.
2. Once all live slots are occupied, retain at most 64 additional requests in
   a FIFO.
3. Reject requests beyond that bound with load-shedding semantics without
   damaging the connection.
4. Optionally expire requests that remain queued for more than 5 ms.

The names in this document are deliberate: 32 is the **in-flight limit**, and
64 is the **queued-request limit**. Calling them low/high watermarks suggests
hysteresis, which this policy does not require. The maximum accepted request
count is 96: 32 in flight plus 64 queued.

## Goals

- Transport-independent placement in the fast_thrift client layer.
- A hard bound on request count and retained serialized request bytes.
- Reject work that is certain to be shed before serializing it.
- FIFO promotion with a bounded queue delay.
- No admission-added lock, atomic, map, virtual dispatch, per-request timer, or
  hot-path heap allocation on the EventBase-owner-thread path.
- Exact permit release on response, local timeout, cancellation, serialization
  failure, disconnect, and teardown.
- Opt-in composition. Existing clients are unchanged unless they add the
  handler.

## Non-goals for the first version

- One-way, streaming, or sink RPC admission. The current native fast_thrift
  path carries request-response traffic only; each additional RPC kind needs
  explicit completion semantics.
- A process-wide or cross-EventBase budget.
- Priority scheduling or adaptive concurrency.
- Deferring serialization of queued requests.
- Bounding response-body memory. That remains a receive/transport concern.

## Current boundary

`ThriftClientAppAdapter::sendRequestResponse()` accepts a
`std::unique_ptr<folly::IOBuf> data`. Generated client code has therefore
serialized method arguments before a `ThriftRequestMessage` enters the Thrift
pipeline.

Consequences:

- A normal pipeline handler can bound the number and bytes of queued serialized
  requests.
- A normal pipeline handler cannot prevent serialization of the request it
  rejects.
- Moving the whole queue above serialization would require it to own arbitrary
  generated method arguments after the caller returns. With today's APIs that
  means method-specific owning copies or a type-erased serialization closure,
  both of which add allocations and can retain more memory than the serialized
  request.

Avoiding serialization for **rejected** work does not require deferred
serialization for **queued** work. A small pre-serialization reservation seam
is sufficient.

## Proposed shape

The feature has two cooperating pieces with one per-connection state machine:

1. `ThriftClientAdmissionControlHandler` is a tail-most Thrift duplex handler.
   It owns the in-flight count, bounded FIFO, queue timer callback, and metrics.
2. An optional admission reservation hook on the native generated-client/app
   adapter path reserves one of the 96 count slots before argument
   serialization. The reservation is transferred into the request context
   when the serialized request is submitted.

Conceptually:

```text
generated FastClient method
  |
  | reserve count slot (reject here when the 96-slot bound is full)
  v
serialize method arguments
  |
  v
ThriftClientAppAdapter
  |
  v
ThriftClientAdmissionControlHandler  <-- queue here when 32 are live
  |
  v
graceful drain / request timeout / checksum
  |
  v
Rocket + selected transport (TCP, MetaRoCE, ...)
```

The handler must be the last handler added to the Thrift pipeline, making it
the first outbound handler after the application tail. A queued request must
not yet be counted by graceful drain, armed by the request-timeout handler,
checksummed, framed, assigned a Rocket stream, or handed to a transport.

Both the ordinary TCP and MetaRoCE client compositions install the same
handler. No MetaRoCE type appears in its API or implementation.

## Admission state

Illustrative configuration:

```cpp
struct ThriftClientAdmissionControlConfig {
  uint32_t maxInFlightRequests{32};
  uint32_t maxQueuedRequests{64};
  std::chrono::microseconds maxQueueDelay{5000};

  // Required for a real memory bound; value still to be selected.
  size_t maxRetainedRequestBytes;
};
```

The suggested 32/64/5 ms values become defaults only when a caller explicitly
installs the handler. The generic library must not silently turn admission
control on for existing clients.

The per-connection state is EventBase-owned and uses ordinary fields:

- `reserved`: count slots acquired before serialization but not yet presented
  to the handler;
- `inFlight`: forwarded requests awaiting terminal completion;
- `queued`: entries in the FIFO;
- `retainedRequestBytes`: serialized payload bytes held by queued and in-flight
  requests;
- a connection generation, so completion from a retiring generation cannot
  modify a reactivated connection;
- head, tail, and count indices for a fixed-capacity ring;
- plain counters, gauges, and high-water marks for off-path observation.

The invariant is:

```text
reserved + inFlight + queued <= maxInFlightRequests + maxQueuedRequests
```

### Queue storage

Allocate the ring once when the handler is constructed. Do not use
`std::deque` or allocate a queue node per request.

Each occupied slot stores:

- the existing `TypeErasedBox`, moved intact from `onWrite()`;
- its absolute queue deadline;
- its cached serialized payload byte count.

Keeping the box intact follows the one-message-type-per-layer rule and avoids
unboxing and re-boxing `ThriftRequestMessage`. The message and its IOBuf chain
remain move-only. `ThriftRequestMessage` already fits TypeErasedBox's 120-byte
inline limit; no new pipeline message type is needed.

Count limits alone do not prevent OOM: 64 arbitrarily large requests are still
unbounded. The handler therefore also needs a byte budget. The serialized
payload length is computed once and cached. A request that would exceed the
byte budget is rejected after serialization but before it enters the rest of
the pipeline. Rejecting by byte size before serialization would require a
method-specific size pass and is not proposed.

The byte budget should cover queued plus in-flight request payloads. Whether
metadata and fixed per-request overhead are charged approximately or through a
constant surcharge can be decided during implementation; the invariant must
remain conservative.

## Pre-serialization reservation

Generated FastClient code should use an optional, compile-time-detected
adapter capability:

1. Ask the app adapter for a move-only count reservation.
2. If the total count bound is full, complete the request callback immediately
   with load-shedding and return without serializing.
3. Serialize normally after a reservation succeeds.
4. Transfer the reservation into `ThriftRequestContext` when submitting the
   request.
5. If serialization or request-context construction fails, the reservation's
   destructor releases the slot.

The capability remains optional so other generated-client adapters and clients
without this handler do not change behavior. The disabled path should compile
to no admission machinery; the enabled path should add a predictable branch
and plain owner-thread counter operations.

The reservation does not decide permanently whether the request will be live
or queued. The handler makes that decision when the serialized message reaches
it, using the current `inFlight` count. This avoids stale decisions if a request
completes during request construction.

### Cross-thread submissions

The zero-atomic design assumes generated client calls originate on the
pipeline's EventBase thread, as the uCache/mcrouter path does. The existing app
adapter also permits off-thread calls by queueing an already serialized request
onto the EventBase. A flood of those callbacks can accumulate before any
pipeline handler runs.

The first implementation should either:

- require EventBase-owner-thread submission when admission control is enabled;
  or
- explicitly accept one atomic reservation operation for the cross-thread
  path.

Silently falling back to post-serialization admission off-thread is not an OOM
guarantee, because the EventBase notification queue itself would remain
unbounded. The owner-thread-only contract is the recommended first scope; it
keeps the common path allocation-, lock-, and atomic-free.

## Outbound state transitions

When a request reaches the handler:

1. Validate or acquire its count reservation.
2. Compute and cache its serialized payload bytes.
3. If the byte budget would be exceeded, reject it with load-shedding.
4. If `inFlight < maxInFlightRequests`, mark its completion permit active,
   increment `inFlight`, and forward the same `TypeErasedBox`.
5. Otherwise append the box to the fixed ring and arm the queue deadline if it
   became the oldest queued request.

When an active request completes, release its count/byte permit, then promote
FIFO entries until either the live limit is full or the queue is empty.
Promotion forwards the original box; it does not copy or reserialize.

Transport `Backpressure` means the forwarded request was accepted by the next
layer but cannot make immediate progress. It remains in flight. Transport
write readiness and admission readiness are separate mechanisms.

## Exact completion accounting

Counting only inbound responses is insufficient. The current request timeout
path cancels a Rocket stream and destroys its `ThriftRequestContext` without
emitting an inbound `ThriftResponseMessage` through every Thrift handler.
Connection teardown can do the same for many streams at once.

`ThriftRequestContext` should therefore carry an inline, allocation-free
completion permit consisting of an owner pointer, function pointer, connection
generation, retained byte count, and armed bit. Destroying or explicitly
completing the context releases the permit exactly once. This follows the
existing context-carried timeout/cancel pattern and avoids a correlation map.

The permit must cover:

- normal response;
- local request timeout and Rocket cancellation;
- transport or connection teardown;
- in-process serialization/framing failure;
- pipeline close;
- a queued request rejected or expired before transport submission.

Lifecycle ordering must be tested: all request contexts must release or be
disarmed before the admission state is destroyed. The generation prevents an
old context from decrementing a newly activated connection.

Promotion should be driven on the owning EventBase. If releasing a context can
occur from a nested cancellation/destruction stack, use one embedded
allocation-free loop callback to coalesce queue draining after that stack
unwinds.

## Queue expiry and clock

Use one `folly::HHWheelTimerHighRes` instance per EventBase, stored in a
`folly::EventBaseLocal` by the connection factory, following
`ConnectionFactory::timerFor`. The handler receives a reference to that shared
timer. It must not construct a timer per connection or a callback per request.

The timer is what makes queue expiry active rather than observational. A clock
read can tell the handler that an entry is stale the next time some request or
response happens, but it cannot wake an otherwise idle EventBase. Without a
scheduled timeout, a queued request could remain retained indefinitely when
the in-flight requests are hung and no new traffic arrives. The high-resolution
timer wakes the EventBase at the oldest queued request's deadline so the
handler can fail it promptly.

The handler owns one embedded high-resolution timer callback. Because the
queue is FIFO and the initial policy has one fixed queue-delay limit, only the
oldest deadline needs to be scheduled. When it fires, expire every due prefix
entry, then arm it for the new head.

For correctness, store absolute `std::chrono::steady_clock` deadlines. The
high-resolution wheel itself uses steady-clock time, so this avoids clock-domain
conversion and remains transport-independent. MetaRoCE's
`HardwareTimestampClock` cannot be imported into generic fast_thrift: it uses
MetaRoCE types and an internal cycle-frequency dependency. If timestamp-read
cost is measurable, factor an open-source-compatible hardware clock into a
shared layer and benchmark it separately; do not duplicate a MetaRoCE clock in
the handler.

Queue delay is separate from the existing client request timeout. In the
initial policy, the 5 ms queue timer runs while the request is queued, and the
normal request timeout starts only after promotion. This makes the maximum
client-side duration approximately queue delay plus request timeout and keeps
the existing timeout handler out of queued-request state.

## Error semantics

There is no load-shedding member in `TTransportException`. The canonical
Thrift representation is:

```cpp
apache::thrift::TApplicationException::LOADSHEDDING
```

Use that for count- or byte-limit rejection. Deliver it as an in-process,
per-request failure; do not call `fireException`, close the pipeline, or mark
the transport unhealthy.

Use `TTransportException::TIMED_OUT` for a local admission-queue expiry, matching
the existing fast_thrift client request-timeout path. Keeping overload and
timeout distinct lets callers choose retry/failover policy correctly.

The uCache route must map `TApplicationException::LOADSHEDDING` to
`carbon::Result::BUSY` when it adopts the generic handler. Its current error
translation does not do that. The existing route-local cap can then be removed.

## Shutdown and drain behavior

- Once graceful drain begins, reject new reservations with retryable
  not-open/draining semantics rather than enqueueing them.
- Fail every queued request on pipeline inactivity; queued work has never
  reached the transport and cannot complete otherwise.
- Cancel the single queue timer and pending loop callback during handler
  removal.
- Let active request contexts release their own permits as the lower pipeline
  retires streams.
- Reset counters only after the old connection generation has been retired.

Admission must be tailward of graceful drain. Otherwise graceful drain counts
queued requests as live and can wait forever for work that admission has not
forwarded.

## Scope of the bound

The proposed state is per connection. With `N` connections, the aggregate
count bound is `N * 96`, and the aggregate byte bound is `N` times the configured
per-connection budget. This matches pipeline ownership and keeps the hot path
single-threaded.

A client-wide budget shared across connections or EventBases would require a
separate coordinator and synchronization. It should be added only if process
capacity cannot be bounded through the configured connection count and
per-connection byte budget.

## Observability

Maintain plain owner-thread counters/gauges and publish snapshots off the
request path:

- admitted immediately;
- queued;
- promoted;
- shed by count;
- shed by bytes;
- expired in queue;
- current and peak in-flight requests;
- current and peak queued requests;
- current and peak retained request bytes;
- queue residence time for promoted and expired requests;
- reservation releases by response, timeout/cancel, and teardown.

No ODS call, lock, atomic, or user callback belongs in the owner-thread handler
path.

## Verification required for implementation

Every test and benchmark must have a checked-in whole-target timeout.

Focused handler tests:

- first 32 requests forward, next 64 queue, request 97 is load-shed;
- FIFO promotion as responses complete;
- count and byte boundaries at exactly limit minus one, limit, and limit plus
  one;
- one queue timer for multiple entries and expiry of a due FIFO prefix;
- queue timeout returns `TIMED_OUT` without closing the connection;
- normal response, request timeout, cancellation, serialization failure,
  disconnect, and teardown each release exactly one permit;
- stale-generation completion cannot corrupt a reactivated connection;
- transport backpressure does not consume a second admission slot;
- graceful drain rejects new work and does not count queued work as live;
- ring wraparound and sustained queue churn;
- a backpressure soak materially exceeding 96 submissions proves retained
  count and bytes remain bounded.

Integration coverage:

- the same handler test pipeline over ordinary TCP and MetaRoCE;
- the uCache route observes load shedding as `carbon::Result::BUSY`;
- generated-client reservation rejection occurs before the serializer;
- owner-thread contract enforcement, plus cross-thread behavior if that mode is
  supported.

Benchmarks:

- handler disabled versus enabled but below the live limit;
- steady state at depths 1, 8, 32, and 64;
- queue/promotion churn at 33 and 96 accepted requests;
- sustained shedding above 96;
- allocations, cycles/request, throughput, and p50/p99/p99.9 latency;
- before/after uCache MetaRoCE results using the existing client benchmark.

The implementation should not replace the route-local cap until the focused
tests, soak, and benchmark delta pass.

## Recommended initial decisions

1. Implement the generic tail-most handler plus pre-serialization count
   reservation. Do not implement deferred serialization.
2. Use 32 in flight, 64 queued, and 5 ms maximum queue delay for the first
   explicit uCache configuration; keep the generic feature opt-in.
3. Require a configured retained-byte limit before calling the mechanism an OOM
   bound. Select the initial number from the uCache request-size distribution
   and configured connection count rather than inventing a universal library
   default.
4. Scope the first version to EventBase-owner-thread submissions, preserving a
   plain-state hot path. Decide separately whether generic off-thread admission
   warrants one atomic reservation operation.
5. Use `TApplicationException::LOADSHEDDING` for capacity rejection and
   `TTransportException::TIMED_OUT` for queue expiry.

## Open decisions

- What retained-byte limit should the initial uCache configuration use, and is
  it one combined queued-plus-in-flight budget or two independently tunable
  budgets?
- Is per-connection admission sufficient, given the configured maximum number
  of client connections, or is a second client-wide budget required?
- May the first version require owner-EventBase calls, or must off-thread
  generated-client calls receive the same pre-serialization guarantee?
- Should queue delay be a connection-only setting initially, or should a later
  change add a per-RPC override distinct from the server's existing
  `queueTimeoutMs` metadata?

## Relevant current code

- `ucache/server/xdp/ThriftMetaRoceRoute.cpp`: route-specific cap being
  replaced.
- `thrift/client/ThriftClientAppAdapter.{h,cpp}`: native generated-client tail
  and the post-serialization boundary.
- `thrift/client/Messages.h`: request/response messages retained by the
  handler.
- `thrift/client/common/ThriftRequestContext.h`: existing per-request
  round-trip state; proposed home of the inline completion permit.
- `thrift/client/handler/ThriftClientGracefulDrainHandler.h`: transport-agnostic
  per-request rejection pattern and ordering constraint.
- `thrift/client/handler/ThriftClientRequestTimeoutHandler.h`: local timeout
  and cancellation path that completion accounting must cover.
- `thrift_fb/pipelined_sr/connection/ConnectionFactory.h`: TCP and negotiated
  MetaRoCE compositions plus the shared high-resolution timer pattern.
