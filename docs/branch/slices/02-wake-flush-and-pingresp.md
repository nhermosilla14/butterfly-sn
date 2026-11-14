# Wake-up flush and PINGRESP

- Intents: 20261114-sleep-and-buffering
- ADRs: 20261114-sleep-state-and-buffer-flush
- Depends on: 01

## Scope
A sleeping client wakes with PINGREQ or CONNECT and receives its buffered messages in order. Unknown topics are
registered first, PINGRESP is sent only after the flush, and buffered QoS 1 messages are never acknowledged to
the broker twice. The client returns to sleep after a PINGREQ wake, or stays active after CONNECT.

## Layers touched
- Services: wake mode, buffer flush through the existing downlink and REGISTER hold paths, PINGRESP ordering,
  pre-acknowledged QoS 1 map, CONNECT-from-asleep transition, buffer reset and sleep timer restart.
- Interface: none.

## End-to-end test
- A client sleeps, a publisher sends several messages (including QoS 1 and a topic never registered), and the
  client wakes with PINGREQ: REGISTER is handled if needed, all payloads arrive in order, and PINGRESP is the
  last datagram before the client sleeps again.
- CONNECT from asleep gets CONNACK Accepted, then the flush, and the session stays active for normal downlink.
- An empty-buffer PINGREQ gets PINGRESP immediately.
- Unit tests prove a buffered QoS 1 message gets no second MQTT PUBACK after the client PUBACK.
- Covers criteria: wake flush, ordering, REGISTER during flush, PINGRESP last, CONNECT wake, no double ack.

## Out of scope
- Retransmission of REGISTER/PUBLISH/PINGREQ.
- Persistence, QoS 2, discovery, will updates.
