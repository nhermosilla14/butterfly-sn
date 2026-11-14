# Reconnect from the same address replaces the session

- Status: Proposed
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Spec area: transport-and-sessions.md

## Context
Sessions are keyed by the client's UDP address. A CONNECT from an address that already has an active session is
ignored without CONNACK, so a client that reboots on the same port waits until the old session times out (1.5
times its keep-alive, never with keep-alive 0). MQTT-SN 6.2 expects CONNACK for every CONNECT. The new CONNECT may
carry another duration, will or client id, and the old broker connection with the same client id would be taken
over by the broker anyway.

## Decision
- The session goroutine decides. A CONNECT that reaches an active session (not asleep) and arrived after that
  session's own CONNACK was sent replaces it: it sends MQTT DISCONNECT to the broker (so the broker publishes no
  will, the client is reconnecting and not lost), does not send DISCONNECT to the client, and asks the gateway to
  replace it with a session for that CONNECT. Everything the old session held is dropped with it.
- The gateway does the swap atomically under its mutex: remove the old session and register the new one for the
  same key, so no other packet sees an empty slot in between and `max_clients` counts one session. A packet
  routed to the old session after it decided is dropped with it; the client is waiting for CONNACK and
  retransmits if needed.
- A CONNECT that arrived before the session's CONNACK (a retransmission of the CONNECT that started it, queued
  while the broker was dialled) is ignored, as is a CONNECT during the will handshake. This is decided by comparing
  the datagram's arrival time with the CONNACK time.
- A CONNECT from a sleeping client keeps its wake meaning, unchanged.
- The new session runs the normal connect path: will handshake if requested, new broker connection, new
  duration, CONNACK Accepted or Congestion. If the new connect fails, the client has no session, like any failed
  CONNECT.
- Logged with client id as "client reconnected, replacing session".

## Alternatives considered
- Answer CONNACK and keep the old session — leaves the old duration, will and subscriptions in force while the
  client believes it started fresh.
- Close the old broker connection bare — makes the broker publish the old will although the client is alive.
- Decide in the router — the router does not know the session's connect state or whether the CONNECT is a
  retransmission, and would need to reach into goroutine-owned state.
- Reject the CONNECT with Congestion until the old session expires — keeps the stall.

## Consequences
A rebooted client connects at once. A retransmitted CONNECT that arrives after CONNACK (a lost CONNACK) also
restarts the session: safe, costing one extra broker connection, and the client then has the CONNACK it lost.
A hostile sender cannot do more than any sender who can already spoof a CONNECT from that address.
