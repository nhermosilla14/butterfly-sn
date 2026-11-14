# Broker event stream for downlink

- Status: Accepted
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: topics-and-pubsub.md, transport-and-sessions.md

## Context
The gateway currently reads only PUBACK, PINGRESP and DISCONNECT from each client's broker connection. Downlink
needs SUBACK, UNSUBACK and incoming PUBLISH from that same connection, in arrival order, while the session
goroutine keeps owning all state. The MQTT client's existing observer API (`Subscribe` blocking on SUBACK,
`ReadPublish`) cannot be combined with the gateway's event loop.

## Decision
- `ReadEvent` becomes the single ordered reader for the gateway connection and gains:
  - `EventSuback` with packet id, granted QoS and return code.
  - `EventUnsuback` with packet id.
  - `EventPublish` with topic, payload (copied), QoS, retain, DUP and packet id (0 for QoS 0).
- PUBLISH validation is strict and bounded: valid topic name, non-empty, no wildcard characters, QoS 0 or 1,
  packet id nonzero for QoS 1. QoS 2, malformed flags or a body shorter than its topic length are
  `ErrMalformedPacket`, which the gateway treats as broker loss.
- New send operations that do not wait for their acknowledgement, because the session reader owns all reads:
  - `SubscribeRequest(filter string, qos byte, id uint16) error` sends SUBSCRIBE with the caller's packet id.
  - `UnsubscribeRequest(filter string, id uint16) error` sends UNSUBSCRIBE.
  - `Puback(id uint16) error` acknowledges one QoS 1 PUBLISH received from the broker.
- The existing observer operations (`Subscribe` waiting for SUBACK, `ReadPublish` for QoS 0) stay for tests and
  tools. They use their own connection; a connection must use either the event reader or the observer reads,
  never both concurrently.
- The library keeps no subscriptions, no pending messages and no MQTT-SN knowledge. Packet ids are chosen by
  the caller.

## Alternatives considered
- Separate `ReadSuback`/`ReadPublish` calls like `ReadPuback` was — the caller cannot know which packet arrives
  next, so it would guess and turn normal traffic into malformed errors.
- A blocking `Subscribe` for the gateway — the session goroutine would wait for SUBACK and stop handling client
  packets, broker pings and timeouts.
- A callback or goroutine per operation inside the client — duplicates the session's serialized event loop and
  adds races.
- Exporting raw packets and MQTT type constants — leaks protocol decoding into the gateway.

## Consequences
One reader path covers every broker packet, with unit tests per event and malformed case. The gateway reader
becomes a dispatcher, and subscription state lives in the session. The library gains a small public surface that
later sleep and retransmission work can reuse. The observer API remains but is documented as separate.
