# TCP Stream Data Plane MVP

This document defines the first Aeron-style data-plane extension for Tina. The MVP deliberately uses TCP instead of UDP so the implementation can focus on the programming model: bounded streams, explicit backpressure, positions, polling, and zero-allocation hot paths.

## Design position

Tina mailboxes remain the control plane. The stream buffer is the data plane.

```
Tina mailbox  -> lifecycle, open/close, supervision, counters, wakeups
Stream buffer -> high-volume ordered payload data
TCP socket    -> reliable byte transport between stream buffers
```

The data path must not allocate Tina message envelopes per application payload. Isolates own and supervise publications/subscriptions; they do not carry every data message in their mailbox.

## MVP scope

Included:

- single TCP connection
- single publication
- single subscription
- single `stream_id`
- single `session_id`
- fixed binary frame header
- bounded publication term buffer
- bounded receive term buffer
- explicit `offer` return codes
- poll-based subscription API
- monotonic positions
- counters
- reader-side one-copy policy

Deferred:

- UDP
- NAK/retransmit
- multicast/MDC
- multiple streams multiplexed on one TCP connection
- archive/replay
- fragmentation/reassembly
- schema/codecs
- cluster consensus

## Zero-copy policy

TCP receive cannot preserve true end-to-end zero-copy in the general case. The practical receive path is:

```
NIC/kernel -> Tina reactor buffer -> receive term buffer -> subscription handler
```

The MVP accepts exactly one controlled userspace copy on receive: from the Tina reactor buffer into a shard-owned receive term buffer. This preserves bounded lifetime, deterministic reclamation, and poll semantics.

The send path is designed so a future `try_claim` API can be zero-copy from application into the publication term buffer:

```
application writes directly into claimed term-buffer region -> TCP writer drains it
```

## Frame layout

Every frame starts with a 32-byte little-endian header:

```odin
Frame_Header :: struct {
    frame_length: u32, // header + payload, not including alignment padding
    frame_type:   u16,
    flags:        u16,
    stream_id:    Stream_Id,
    session_id:   Session_Id,
    position:     Position,
    reserved:     u64,
}
```

Frame types:

- `DATA` carries application payload
- `PADDING` advances to the start of the ring when a frame would cross the end

Frames are aligned to 8 bytes in the term buffer.

## Send path

```
Business isolate
  publication_offer(payload)
    -> append DATA frame to publication term buffer
    -> returns Position or Offer_Error

TcpWriter isolate
  publication_drain_contiguous()
    -> gets contiguous bytes already framed
    -> submits ctx_io_send
  on send complete
    -> publication_commit_drained(n)
```

The writer never blocks in user code. If the TCP socket cannot accept data, the writer parks on Tina I/O completion. The publication becomes back-pressured when producer position advances too far ahead of writer position.

## Receive path

```
TcpReader isolate
  receives bytes into Tina reactor buffer
  tcp_receive_push(bytes)
    -> parses length-prefixed frames
    -> copies complete DATA frames into receive term buffer
    -> stops reading when receive term buffer is full

Business isolate
  subscription_poll(fragment_limit)
    -> reads DATA frames from receive term buffer
    -> invokes handler with payload slice valid only for the poll call
```

When the receive term buffer is full, the TCP reader should stop posting reads. TCP backpressure then propagates through the kernel to the remote writer.

## Backpressure

`publication_offer` fails before the kernel socket buffer becomes the only limiter:

```
producer_position - writer_position >= publication_capacity -> back_pressured
```

`ctx_send`-style policy remains explicit. The producer decides whether to retry, shed load, crash, or propagate backpressure upstream.

## MVP build steps

1. Add core stream/frame/term-buffer types.
2. Implement `publication_offer`.
3. Implement contiguous drain/commit for TCP writer.
4. Implement `subscription_poll`.
5. Implement TCP receive parser with one-copy append into receive term buffer.
6. Add counters.
7. Wire a TCP writer isolate and TCP reader isolate around Tina I/O.
8. Add deterministic in-memory loopback tests before real sockets.

## Non-goals

This is not an Aeron clone. It is an Aeron-style programming model over TCP, using Tina's scheduling, supervision, bounded memory, and deterministic testing model.
