# Will handshake and broker will

- Status: Done
- Intents: 20261114-will-handshake-and-publication
- ADRs: 20261114-will-handshake-and-broker-mapping
- Depends on: none

## Scope
A client can connect with a will. The gateway asks for the will topic and message before it contacts the broker,
carries that will in the broker CONNECT, and answers CONNACK only after the broker accepted. An empty will topic
connects without a will, QoS 2 wills are refused, and an unfinished handshake has a bounded lifetime.

## Layers touched
- Services: `internal/mqttsn` WILLTOPICREQ, WILLTOPIC, WILLMSGREQ and WILLMSG codec; `butterfly-mqtt-client`
  `ConnectOptions` will fields and CONNECT encoding; session handshake phase before the broker dial, with its
  timer and stored will state.
- Interface: e2e helper to run the will handshake and expect each prompt.

## End-to-end test
- CONNECT with Will=1 gets WILLTOPICREQ; WILLTOPIC with a topic and QoS 1/retain gets WILLMSGREQ; WILLMSG gets
  CONNACK Accepted, and the broker connection was opened with that will.
- An empty WILLTOPIC gets no WILLMSGREQ and still gets CONNACK Accepted.
- A WILLTOPIC asking for QoS 2 gets CONNACK Not supported and creates no session.
- An unanswered WILLTOPICREQ ends with DISCONNECT and no session.
- Covers criteria: handshake order, empty topic, timeout, QoS 2 refusal, CONNACK after the broker.

## Out of scope
- Publishing the will: the next slice changes the teardown paths.
- WILLTOPICUPD/WILLMSGUPD, persistence across sessions, sleep.
- QoS 2, retransmission, subscriptions and downlink.
