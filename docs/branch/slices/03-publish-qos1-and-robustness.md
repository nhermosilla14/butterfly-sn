# Publish at QoS 1 and handle bad input

- Status: In progress
- Intents: 20261113-walking-skeleton
- ADRs: 20261113-package-layout-and-session-concurrency, 20261113-own-minimal-mqtt-client, 20261113-end-to-end-testing-with-real-broker
- Depends on: 02

## Scope
QoS 1 publishes are acknowledged to the client once the broker accepted them. The gateway also behaves safely
with bad input: packets from unknown or non-connected senders get DISCONNECT, client ids are matched exactly
and empty ones are rejected, and malformed datagrams are ignored and logged.

## Layers touched
- Services: `butterfly-mqtt-client` PUBLISH QoS 1 with PUBACK wait; `internal/mqttsn` PUBACK; session maps the client's message id to the broker's and returns PUBACK with message id and topic id; router decision for unknown senders; exact client-id lookup; length and framing validation; fuzz targets for both codecs.
- Interface: e2e helper gains raw-datagram sending for malformed cases.

## End-to-end test
- PUBLISH QoS 1 yields a PUBACK with the client's msg id and topic id after the broker has the message.
- A non-CONNECT packet from an unknown sender and from a disconnected client each get DISCONNECT.
- A CONNECT with an empty client id is rejected; a client id that is only a prefix of a connected one creates a separate session.
- Zero-length, truncated and oversized datagrams do not crash the gateway, and a following valid CONNECT still works.
- Covers criteria: PUBLISH QoS 1, unknown or disconnected senders, exact and empty client id, malformed datagrams.

## Out of scope
- QoS 2, retransmission of unacknowledged PUBLISH, will, sleep.
