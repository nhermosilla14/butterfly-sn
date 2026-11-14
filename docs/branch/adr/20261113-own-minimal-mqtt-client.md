# Own minimal MQTT 3.1.1 client towards the broker

- Status: Proposed
- Date: 2026-11-13
- Deciders: Nicolás Hermosilla
- Spec area: transport-and-sessions.md, topics-and-pubsub.md

## Context
In transparent mode each MQTT-SN client gets its own broker connection, and the gateway must carry MQTT-SN
behaviour onto it precisely: client id, will, clean session, keepalive, and QoS 1 flows with message ids passed
through. The old gateway spoke MQTT 3.1.1. Existing Go clients (such as paho.mqtt.golang or paho.golang) manage
their own reconnects, session state and message ids, which gets in the way of a protocol translator, and they
pull in dependencies.

## Decision
Implement a small MQTT 3.1.1 packet codec and connection in `internal/mqtt`, standard library only. In scope
now: CONNECT, CONNACK, PUBLISH, PUBACK, PINGREQ, PINGRESP, DISCONNECT. SUBSCRIBE, SUBACK, UNSUBSCRIBE,
UNSUBACK and PUBREC/PUBREL/PUBCOMP are added by later intents. Packets are read from the TCP stream with full
framing (the fixed header's variable-length size is read, then exactly that many bytes), fixing the old
gateway's assumption that a packet arrives in one `recv`. The broker address is a plain TCP `host:port`, with
a connect timeout and no credentials in this version (TLS and auth are non-goals).

## Alternatives considered
- `eclipse/paho.mqtt.golang` — auto-reconnect and internal session handling conflict with per-client translation; extra dependency.
- `eclipse/paho.golang` (MQTT 5) — heavier, v5-centred, and the target brokers use 3.1.1.

## Consequences
No third-party MQTT dependency and full control of the flows. We own correctness of the codec, so it needs
unit and fuzz tests plus the end-to-end test against a real broker.
