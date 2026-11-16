# When the will is published

- Status: Accepted
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: will.md, transport-and-sessions.md

## Context
MQTT brokers publish a will when a client connection ends without MQTT DISCONNECT. The gateway currently sends
MQTT DISCONNECT on client keepalive timeout and on shutdown, which would suppress the will. The vision requires
the will to be published when the client is lost abnormally and when the gateway shuts down. The broker must do
the publishing: it owns the will QoS, retain and ordering, and the gateway has no business re-publishing it.

## Decision
The gateway chooses how to end the broker connection according to why the session ends:
- Explicit client DISCONNECT: send MQTT DISCONNECT, then close. No will is published.
- Client keepalive timeout: remove the session, then close the broker socket without MQTT DISCONNECT when the
  session has a will, so the broker publishes it. A will-less session keeps the graceful MQTT DISCONNECT. The
  client then gets the best-effort MQTT-SN DISCONNECT as today.
- Gateway shutdown (SIGINT/SIGTERM): sessions with a will close their broker socket without MQTT DISCONNECT;
  will-less sessions send MQTT DISCONNECT. Every client still gets a best-effort MQTT-SN DISCONNECT, as now.
- Broker connection loss: the broker already detects the closed connection and publishes the will. The gateway
  only takes its existing broker-loss path (notify the client, remove the session) and never publishes the will
  itself.
- CONNECT refused by the broker or a will handshake timeout: no broker connection is left open, so there is
  nothing to publish.

This refines the earlier session-liveness decision for will-bearing sessions only: the graceful MQTT DISCONNECT
on timeout and shutdown remains for sessions without a will. The rest of that decision is unchanged.

## Alternatives considered
- Publish the will from the gateway before closing — the broker would publish it too, producing duplicates, and
  the gateway would have to reproduce QoS and retain ordering.
- Always close abruptly on timeout and shutdown, even without a will — harmless for will-less sessions but loses
  the clean MQTT signal for no benefit.
- Send MQTT DISCONNECT and publish the will ourselves — the broker suppresses the stored will, and doing the
  broker's job duplicates policy in the gateway.

## Consequences
The broker must have will support enabled, which Mosquitto does by default. A gateway shutdown now intentionally
triggers client wills; operators see that as the required v1 behaviour. Teardown has one conditional (has will)
in the timeout and shutdown paths, and tests must observe the will through an independent subscriber. No extra
packets are sent to clients, and the existing broker-loss tests stay valid.
