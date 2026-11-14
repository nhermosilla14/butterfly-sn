# Publish at QoS 1 and handle bad input

- Status: Done
- Intents: 20261113-walking-skeleton
- ADRs: 20261113-package-layout-and-session-concurrency, 20261113-own-minimal-mqtt-client, 20261113-end-to-end-testing-with-real-broker
- Depends on: 02

## Scope
QoS 1 publishes are acknowledged to the client once the broker accepted them. The gateway also behaves safely
with bad input: packets from unknown or non-connected senders get DISCONNECT, client ids are matched exactly
and empty ones are rejected, and malformed datagrams are ignored and logged.

## Layers touched
- Services: `butterfly-mqtt-client` PUBLISH QoS 1 with PUBACK wait; `internal/mqttsn` PUBACK; session reuses the client's message id as the broker packet id and returns PUBACK with message id and topic id; router decision for unknown senders; exact client-id lookup; length and framing validation; fuzz targets for both codecs.
- Interface: e2e helper gains raw-datagram sending for malformed cases.

## End-to-end test
- PUBLISH QoS 1 yields a PUBACK with the client's msg id and topic id after the broker has the message.
- A non-CONNECT packet from an unknown sender and from a disconnected client each get DISCONNECT.
- A CONNECT with an empty client id is rejected; a client id that is only a prefix of a connected one creates a separate session.
- Zero-length, truncated and oversized datagrams do not crash the gateway, and a following valid CONNECT still works.
- Covers criteria: PUBLISH QoS 1, unknown or disconnected senders, exact and empty client id, malformed datagrams.

## Behaviour notes
- An empty client id makes CONNECT invalid: reply DISCONNECT without creating a session or dialing the broker,
  matching the legacy invalid-CONNECT patch. This replaces slice 01's temporary silent-ignore behaviour.
- QoS 1 message ids pass through unchanged on each client's dedicated broker connection; the session remembers the
  topic id so the broker PUBACK can be translated back with the original msg id and topic id.
- A QoS 1 PUBLISH with msg id 0, with an unknown topic id, or reusing a msg id that is still awaiting its broker
  PUBACK is dropped and logged without a client reply. Retransmission and an invalid-topic-id PUBACK belong to
  later intents.
- DISCONNECT from an unknown sender is ignored rather than answered, to avoid an answer loop. Any other valid
  packet from an unknown sender gets DISCONNECT, including packet types the codec does not implement yet; those
  are ignored when they come from a live session.
- A broker read error is logged and ends the session's broker reader; the client is not notified yet, since
  broker-drop handling is a later intent.

## Out of scope
- QoS 2, retransmission of unacknowledged PUBLISH, will, sleep.
