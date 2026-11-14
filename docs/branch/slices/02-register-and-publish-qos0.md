# Register a topic and publish at QoS 0

- Intents: 20261113-walking-skeleton
- ADRs: 20261113-package-layout-and-session-concurrency, 20261113-own-minimal-mqtt-client, 20261113-end-to-end-testing-with-real-broker
- Depends on: 01

## Scope
A connected client registers a topic name and publishes QoS 0 messages with the returned topic id; the message
reaches the broker on the topic name unchanged. A QoS 0 PUBLISH with an unknown topic id is dropped.

## Layers touched
- Services: `internal/mqttsn` codec for REGISTER, REGACK, PUBLISH; `butterfly-mqtt-client` PUBLISH (QoS 0); session topic table with id allocation 1 to 0xFFFE, same name returns the same id.
- Interface: e2e helper gains REGISTER and PUBLISH; an observer MQTT client built on `butterfly-mqtt-client` subscribes to check delivery.

## End-to-end test
- REGISTER returns REGACK Accepted with an id; registering the same name again returns the same id.
- PUBLISH QoS 0 with that id arrives at the broker on the right topic with an identical payload.
- PUBLISH QoS 0 with an unknown id sends nothing to the broker.
- Covers criteria: REGISTER, PUBLISH QoS 0, unknown topic id dropped.

## Out of scope
- QoS 1, subscriptions and broker-to-client delivery, short topic names, topic table limit handling.
