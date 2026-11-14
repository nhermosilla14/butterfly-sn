# Own minimal MQTT 3.1.1 client towards the broker

- Status: Accepted
- Date: 2026-11-13
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: transport-and-sessions.md, topics-and-pubsub.md

## Context
In transparent mode each MQTT-SN client gets its own broker connection, and the gateway must carry MQTT-SN
behaviour onto it precisely: client id, will, clean session, keepalive, and QoS 1 flows with message ids passed
through. The old gateway spoke MQTT 3.1.1. Existing Go clients (such as paho.mqtt.golang or paho.golang) manage
their own reconnects, session state and message ids, which gets in the way of a protocol translator, and they
pull in dependencies.

## Decision
The broker side speaks MQTT 3.1.1 only; MQTT 5 is not planned. Implement a small MQTT 3.1.1 packet codec and connection as the standalone module `butterfly-mqtt-client/` (not under `internal/`, so it can later move to its own repository; see the package layout ADR), standard library only, with no knowledge of MQTT-SN or the gateway. In scope
now: CONNECT, CONNACK, PUBLISH, PUBACK, PINGREQ, PINGRESP, DISCONNECT. QoS 0 SUBSCRIBE/SUBACK and
incoming QoS 0 PUBLISH reads are also included for the e2e observer required by slice 02; this does not add
subscription handling to the gateway. Other subscription operations and PUBREC/PUBREL/PUBCOMP are added by
later intents. Packets are read from the TCP stream with full
framing (the fixed header's variable-length size is read, then exactly that many bytes), fixing the old
gateway's assumption that a packet arrives in one `recv`. The broker address is a plain TCP `host:port`, with
a connect timeout and no credentials in this version (TLS and auth are non-goals).

## Alternatives considered
- `eclipse/paho.mqtt.golang` — auto-reconnect and internal session handling conflict with per-client translation; extra dependency.
- `eclipse/paho.golang` (MQTT 5) — v5 only, manages packet ids itself, and the target brokers are used with 3.1.1.
- `eclipse/paho.mqtt.golang/packets` or `mochi-mqtt/server/v2/packets` as a bare codec — rejected by the owner to avoid pulling in the dependency graphs of those modules.
- `at-wat/mqtt-go` — small, but little adopted and its QoS 1 handling is unverified.

## Consequences
No third-party MQTT dependency and full control of the flows. We own correctness of the codec, so it needs
unit and fuzz tests plus the end-to-end test against a real broker.
