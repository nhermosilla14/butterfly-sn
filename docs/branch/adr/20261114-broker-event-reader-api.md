# Broker event reader API in the MQTT client

- Status: Accepted
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: transport-and-sessions.md

## Context
The gateway needs to read several broker packet types from the same connection in arrival order: PUBACK for
QoS 1 publishes, PINGRESP for gateway-originated keepalive, and MQTT DISCONNECT. The client module currently
offers `ReadPuback`, which fails on any other type, so a keepalive response or a broker DISCONNECT would be
reported as a malformed packet and end the reader. The module has no PINGREQ send operation. The reader API must
stay policy-free: timers, pending packet ids and client notification belong to the gateway.

## Decision
- Add MQTT PINGREQ and PINGRESP packet types and a `Pingreq()` operation that sends a PINGREQ with no body.
- Replace `ReadPuback` with `ReadEvent(ctx) (Event, error)`, a single reader operation that returns the next
  broker event in arrival order. `Event` is a small typed value:
  - `Puback` with the packet identifier.
  - `Pingresp`.
  - `Disconnect` (the broker closed the MQTT session).
  Validation stays strict: PUBACK must have flags 0 and a 2-byte body, PINGRESP flags 0 and an empty body,
  DISCONNECT flags 0 and an empty body. Any other packet, or a malformed one, is `ErrMalformedPacket`.
- `ReadEvent` is bounded by the context deadline and cancellation, using the same read machinery as the other
  reads. Only one reader operation may run at a time; reads must not run concurrently with `ReadPublish` or
  `Subscribe`.
- The client keeps no timers, no outstanding-ping state and no knowledge of MQTT-SN. The gateway decides when to
  ping and when a missing PINGRESP means loss.

## Alternatives considered
- Separate `ReadPingresp` and `ReadPuback` calls — the caller cannot know which packet comes next, so it would
  have to guess or peek, ending in spurious malformed errors.
- Export the raw `Packet` and its type constants and let the gateway decode — leaks protocol details into the
  gateway and duplicates validation.
- A callback/event-loop API inside the client — more machinery than one connection and one reader need.
- Keep `ReadPuback` and add a second raw read path — two read paths race and both need deadline handling.

## Consequences
One ordered read path for all broker events, with tests for each event and malformed case. The client API
changes at v0: `ReadPuback` disappears and its callers move to `ReadEvent`. The gateway reader becomes a small
loop that forwards events; all liveness policy stays in the session. Future downlink packets (PUBLISH, SUBACK)
extend the event type rather than adding reads.
