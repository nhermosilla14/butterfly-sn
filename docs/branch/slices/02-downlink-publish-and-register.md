# Downlink publishes and gateway registration

- Status: Done
- Intents: 20261114-downlink-pubsub
- ADRs: 20261114-broker-event-stream-downlink, 20261114-downlink-delivery-and-topics
- Depends on: 01

## Scope
Broker messages reach the client. Topics already in the client's table are published directly; unknown topics
are announced with a gateway REGISTER and held until REGACK. Downlink QoS 1 is acknowledged to the broker only
after the client acknowledges it, and per-client state stays bounded.

## Layers touched
- Services: session broker-event dispatch for PUBLISH, gateway REGISTER with a per-session msg id, bounded hold
  list, pending downlink QoS 1 map, MQTT PUBACK send, 50-name table cap with Congestion answers.
- Interface: none beyond the existing UDP and broker connections.

## End-to-end test
- A subscribed topic receives an exact broker PUBLISH with payload, retain and QoS preserved, no REGISTER.
- A wildcard subscription receives a topic the client never registered: REGISTER arrives first, the test
  REGACKs, then PUBLISH with the new id.
- Downlink QoS 1 reaches the client with the broker packet id; the client PUBACKs and a later publish still
  flows. Unit tests with a fake broker prove the broker PUBACK follows the client PUBACK.
- A full topic table answers REGACK/SUBACK Congestion and drops a broker publish with a broker acknowledgement
  instead of growing.
- Covers criteria: known topic, unknown topic REGISTER, QoS 1 ordering, table bound.

## Out of scope
- Short topic names; next slice.
- Retransmission of REGISTER/PUBLISH, sleep buffering, QoS 2.
