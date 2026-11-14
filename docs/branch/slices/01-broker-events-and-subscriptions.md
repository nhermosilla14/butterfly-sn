# Broker events and client subscriptions

- Status: Done
- Intents: 20261114-downlink-pubsub
- ADRs: 20261114-broker-event-stream-downlink, 20261114-downlink-delivery-and-topics
- Depends on: none

## Scope
A client can subscribe and unsubscribe. The gateway forwards the request on that client's broker connection,
maps the broker answer back with the client's msg id, and reports a usable topic id for exact names and 0 for
wildcards. The broker reader now understands every broker packet the gateway needs.

## Layers touched
- Services: `butterfly-mqtt-client` `ReadEvent` gains SUBACK, UNSUBACK and PUBLISH events, plus
  `SubscribeRequest`, `UnsubscribeRequest` and `Puback`; `internal/mqttsn` SUBSCRIBE, SUBACK, UNSUBSCRIBE and
  UNSUBACK codec; session subscription routing and topic-id rules.
- Interface: e2e helper for SUBSCRIBE/UNSUBSCRIBE and its acknowledgements.

## End-to-end test
- SUBSCRIBE an exact topic gets SUBACK Accepted with granted QoS and a nonzero topic id.
- SUBSCRIBE a wildcard filter gets SUBACK Accepted with topic id 0.
- UNSUBSCRIBE gets UNSUBACK with the client's msg id.
- A broker refusal maps to SUBACK Invalid topic id (0x02) with QoS 0 and topic id 0.
- Covers criteria: subscribe, wildcard id, unsubscribe, broker subscription.

## Out of scope
- Delivering broker PUBLISH messages to the client; next slice.
- Short topic names and the 50-name table bound; later slice.
- Sleep, retransmission, QoS 2.
