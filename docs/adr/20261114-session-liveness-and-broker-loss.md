# Session liveness: client timeout, broker keepalive and broker loss

- Status: Accepted
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: transport-and-sessions.md

## Context
Today the gateway opens one broker connection per client with the client's CONNECT duration as the MQTT
keepalive, but it never sends PINGREQ and never forwards PINGREQ either. An idle client is therefore dropped by
the broker after about 1.5 times its duration, while the gateway keeps the session and its slot forever and the
client is never told. A client that disappears is likewise never timed out. The MQTT-SN specification (6.11,
6.14) makes the gateway responsible for supervising the client's keepalive and allows an unsolicited DISCONNECT.
The vision asks for 1.5x enforcement and for the client to be told when the broker drops.

## Decision
Session liveness is owned by each session goroutine, using timers in its existing event loop. The broker reader
goroutine only turns broker packets into events; it holds no timers or policy.

Client side:
- A CONNECT duration greater than 0 arms an inactivity timer of 1.5 times the duration. Every valid MQTT-SN
  packet from the client refreshes it, including PINGREQ, PUBLISH, REGISTER, PUBACK and DISCONNECT.
- When the timer fires, the gateway sends a best-effort DISCONNECT to the client, sends MQTT DISCONNECT to the
  broker, closes the broker connection and removes the session.
- PINGREQ from an active client is answered with PINGRESP by the gateway and counts as activity. It is not
  forwarded to the broker, because broker liveness is now the gateway's responsibility.
- The gateway does not originate PINGREQ towards active clients; enforcement is passive.
- A duration of 0 disables the inactivity timer, the broker PINGREQ and the MQTT keepalive.

Broker side:
- MQTT CONNECT keeps the client duration as its keepalive, unchanged.
- While the session is active and the duration is greater than 0, the gateway sends MQTT PINGREQ whenever half
  the keepalive has passed since it last wrote to the broker, and expects PINGRESP. A PINGREQ still unanswered
  when the next one is due means the connection is lost; there is no separate retry count.
- A broker connection is also lost on TCP close or read error, write error, a malformed packet, or an MQTT
  DISCONNECT from the broker.
- On any broker loss the gateway sends a best-effort MQTT-SN DISCONNECT to the client and removes the session,
  closing the socket and freeing the slot. Pending QoS 1 acknowledgements are abandoned; retransmission is a
  later intent.

Both endings use the same teardown as an explicit client DISCONNECT: close the broker connection, cancel and
join the reader, remove the session from the router.

## Alternatives considered
- Forward client PINGREQ to the broker, as the legacy gateway did — couples broker liveness to client traffic,
  and still leaves idle clients to be dropped.
- Let the gateway originate PINGREQ towards clients — permitted by the spec, but adds traffic and retransmission
  policy for a supervision rule that only needs client-to-gateway traffic.
- Put keepalive timers inside `butterfly-mqtt-client` — mixes policy with the reusable protocol library and
  duplicates the session's event loop.
- A configurable broker ping interval — one more key without a use case; half the client keepalive is derived
  and predictable.
- Close the broker socket without MQTT DISCONNECT on client timeout — cheap, but hides a normal ending from the
  broker and from its logs.

## Consequences
Idle clients survive and stale sessions disappear, so `max_clients` counts live sessions. Timer accuracy depends
on the session loop not being blocked; broker dialing already has a timeout and packet handling is non-blocking.
Tests can use short durations (1–2 s) to exercise timeouts without slowing the suite. A lost broker is now
visible to the client as a DISCONNECT, which the client can answer by reconnecting. Will publication on a lost
client and sleep-duration supervision remain for later intents.
