# Publish the will on abnormal loss and shutdown

- Intents: 20261114-will-handshake-and-publication
- ADRs: 20261114-will-publication-triggers, 20261114-will-handshake-and-broker-mapping
- Depends on: 01

## Scope
The stored will is actually published by the broker in the situations the vision requires: when a will-bearing
client misses its keepalive, when the gateway shuts down, and when the broker connection is lost. An explicit
client DISCONNECT and a will-less session publish nothing.

## Layers touched
- Services: the client-timeout teardown and shutdown path choose between an abrupt broker close (session has a
  will) and MQTT DISCONNECT (no will). Broker loss keeps its existing path. No new packets or timers.
- Interface: none beyond the UDP and broker interfaces; tests observe the will with an independent subscriber.

## End-to-end test
- A client with a will goes silent: the observer receives the exact will topic and payload, the client gets
  DISCONNECT and the broker connection is gone.
- Gateway shutdown with a will-bearing client: the observer receives the will, the client gets DISCONNECT and
  the process exits 0.
- Explicit DISCONNECT: no will is published within a bounded wait.
- A will-bearing client whose broker connection is cut through the proxy: the observer receives the will and the
  client gets DISCONNECT.
- A client that connected with an empty will topic and then goes silent publishes no will.
- Covers criteria: publication on timeout, shutdown, broker loss; no will on DISCONNECT; empty topic.

## Out of scope
- WILLTOPICUPD/WILLMSGUPD and will persistence across sessions.
- Publishing wills on gateway startup failure or crash.
- QoS 2, retransmission, subscriptions and downlink.
