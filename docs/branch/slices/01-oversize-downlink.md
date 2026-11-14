# Oversize downlink is dropped

- Intents: 20261114-conformance-defect-fixes

## Scope
A broker message whose MQTT-SN datagram exceeds one UDP datagram (65 507 bytes) is dropped and logged, QoS 1 is
acknowledged to the broker, and nothing enters the retry set. Applies to PUBLISH, REGISTER (held messages dropped,
name forgotten) and flushed messages. The client stays connected and its will is not published.

## Layers touched
- Foundation: a send path that reports encode or size failure (`internal/gateway`); the encode limit in
  `internal/mqttsn` stays.
- Services: `handleDownlink`, `sendDownlinkTo`, `flushHead`, `sendFlushed` check before recording a retry.
- Interface: MQTT-SN datagrams.

## End-to-end test
- Gateway tests with a real broker: a QoS 1 broker PUBLISH of about 70 000 bytes to a subscribed client is dropped
  and acknowledged to the broker, the session survives past several retry intervals, no retransmission arrives and
  the next normal message is delivered; same for a QoS 0 one and for a topic name needing REGISTER too large to
  send; same for a buffered message flushed after a wake.
- Covers the D11 criteria.

## Out of scope
- Uplink size limits.
