# Walking skeleton: connect, publish and disconnect


- Status: In progress
- Date: 2026-11-13
- Owner: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla

## Goal
An MQTT-SN client over UDP can connect through the gateway to a plain MQTT broker, publish messages at QoS 0
and QoS 1 to a registered topic, and disconnect. The gateway runs as a single binary configured by a YAML
file, logs to stdout, and stops cleanly on SIGINT and SIGTERM. This establishes the end-to-end path (UDP
listener, session, per-client broker connection, test harness) that later intents extend.

Sources: `olddocs/behavior/session-lifecycle.md`, `olddocs/behavior/pubsub-topics.md`,
`olddocs/behavior/configuration.md`, `olddocs/technical/known-issues.md`.

## Acceptance criteria
- Starting the gateway with a YAML file that sets the UDP listen address and the broker address opens the
  UDP socket and logs a startup line; a missing or invalid file makes it log the error and exit non-zero.
- CONNECT (CleanSession=1, no will) from a client gets a CONNACK with return code Accepted once the broker
  CONNECT/CONNACK succeeded; the broker sees one connection per MQTT-SN client, using the client id.
- If the broker is unreachable, CONNECT gets a CONNACK with return code Congestion, and no state is leaked.
- REGISTER of a topic name gets a REGACK with a topic id (1 to 0xFFFE) and Accepted; the same name registered
  twice returns the same id for that client.
- PUBLISH QoS 0 with that topic id arrives at the broker on the topic name; the payload is unchanged.
- PUBLISH QoS 1 gets a PUBACK to the client after the broker accepted it, carrying the client's msg id and
  topic id.
- A QoS 0 PUBLISH with an unknown topic id is dropped (nothing reaches the broker).
- DISCONNECT closes the broker connection and the client receives DISCONNECT in reply.
- A packet from a sender that is unknown or not connected (and is not CONNECT) is answered with DISCONNECT.
- A client id is matched exactly (a prefix of an existing id does not match) and an empty client id is rejected.
- Datagrams shorter than the declared length, or with length 0, are ignored and logged without crashing.
- SIGINT and SIGTERM close all broker connections and exit with status 0.
- An end-to-end test drives the gateway only through UDP against a real in-process or containerised broker
  and covers the criteria above (the broker choice is an ADR).

## Spec changes
- Adds: transport-and-sessions / Startup and shutdown
- Adds: transport-and-sessions / Connect and disconnect
- Adds: transport-and-sessions / Unknown or disconnected senders
- Adds: topics-and-pubsub / Register topic
- Adds: topics-and-pubsub / Publish QoS 0 and QoS 1 from a client
- Adds: configuration / Minimal YAML configuration

## Out of scope
- Subscriptions and broker-to-client publishes, wildcards, short topic names.
- Will, sleep, keepalive enforcement, client timeouts, slot reclaim, retransmission.
- Discovery (SEARCHGW, GWINFO, ADVERTISE).
- Container image and release builds (separate intent).
- Broker connection drops mid-session (separate intent).
- QoS 2, TLS and authentication to the broker.

## Decisions taken
- CleanSession=0 is accepted and treated as clean in this intent; real handling comes with the sleep intent.
- Minimal config keys: `listen` (UDP host:port), `broker` (host:port), `max_clients`, `log_level`.
