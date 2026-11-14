# Session liveness: keepalive, timeouts and broker-drop handling

- Status: In progress
- Date: 2026-11-14
- Owner: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Depends on: none

## Goal
Sessions reflect reality. The gateway keeps each broker connection alive on its own, answers client keepalive,
expires clients that went silent, and tells a client when its broker connection is gone instead of leaving it
believing it is connected. After any of those endings the session is removed and its slot is available again.

## Acceptance criteria
- An active client that sends no MQTT-SN packet for longer than 1.5 times the keepalive from its CONNECT is
  considered lost: the gateway sends it a best-effort DISCONNECT, ends the session, closes its broker
  connection and frees its client slot.
- The client limit counts live sessions only. With `max_clients` reached, a new CONNECT is refused with
  CONNACK Congestion; once a session ends by timeout or broker loss, a new CONNECT is accepted.
- PINGREQ from an active client is answered with PINGRESP, and counts as activity for its inactivity timer.
- An idle client stays usable: the gateway keeps the broker connection alive itself, so a publish after an idle
  period several times the keepalive still reaches the broker without the client sending PINGREQ.
- A keepalive of 0 disables both the client inactivity timeout and gateway-originated broker keepalive traffic.
- When the broker connection is lost (TCP close, read or write error, MQTT DISCONNECT, or no PINGRESP after the
  gateway's PINGREQ within a bounded wait), the client receives MQTT-SN DISCONNECT and the session is removed,
  freeing its slot and leaving no goroutines behind.
- Timeouts and broker losses are logged with the client identity, and never with payload bytes.
- Gateway shutdown (SIGINT, SIGTERM) still closes every broker connection, notifies clients and exits 0.

## Spec changes
- Adds: transport-and-sessions / Keepalive and liveness
- Adds: transport-and-sessions / Broker connection loss
- Changes: transport-and-sessions / Connect and disconnect
- Changes: transport-and-sessions / Not supported

## Out of scope
- Sleep and the asleep/awake states; DISCONNECT with a duration keeps its current meaning (full disconnect).
- Will, including publishing a will when a client is declared lost.
- Retransmission of REGISTER, PUBLISH, PINGREQ or other unanswered messages.
- Gateway-originated PINGREQ to clients (see open questions).
- Reconnection, session resumption or broker takeover for the same client id.
- Subscriptions and downlink, discovery, QoS 2, TLS and broker authentication.

## Decisions taken
- On client timeout, the gateway sends a best-effort MQTT-SN DISCONNECT before dropping the session. The spec
  allows an unsolicited DISCONNECT, and the legacy gateway's silence is not worth keeping.
- Keepalive is enforced passively: the gateway never originates PINGREQ to an active client, because the rule is
  based on not receiving anything from it. It answers client PINGREQ with PINGRESP.
- The gateway pings the broker at half the client's keepalive, derived from CONNECT, with no new configuration
  key. Keepalive 0 means no pings.
