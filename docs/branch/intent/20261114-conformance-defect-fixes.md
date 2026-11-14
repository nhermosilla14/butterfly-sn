# Conformance defect fixes: re-sleep, repeated CONNECT, oversize downlink

- Status: Accepted
- Date: 2026-11-14
- Owner: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Depends on: none

## Goal
Three defects found by the conformance review (`CONFORMANCE.md` D1, D3, D11) are fixed, so that a sleeping client
can change its sleep duration and go back to sleep, a client that reboots on the same address can connect again at
once, and one oversize broker message can no longer get a healthy client disconnected.

## Acceptance criteria
### Re-sleep (D1, MQTT-SN 6.14 and 5.4.21)
- A DISCONNECT with a duration above 0 from a sleeping client (not flushing) replaces the sleep duration, restarts
  the sleep timer with 1.5 times the new duration, and gets DISCONNECT as the reply. The session, the broker
  connection, the subscriptions and the buffered messages are kept, and no will is published.
- The same DISCONNECT during a wake-up flush (client awake) ends the flush: messages not yet sent stay in the
  buffer in order, REGISTERs and QoS 1 PUBLISHes still awaiting an answer are dropped from supervision, the client
  gets DISCONNECT and is asleep again with the new duration. Messages already acknowledged to the broker are not
  acknowledged twice, and a buffered message is not lost or duplicated by this.
- A later PINGREQ from that client wakes it as usual and receives what was buffered.
- A DISCONNECT without a duration, or with duration 0, from a sleeping client still ends the session gracefully
  with no will (unchanged).

### Repeated CONNECT (D3, 6.2)
- A CONNECT from an address that has a live session in the active state ends that session gracefully (MQTT
  DISCONNECT to the broker, so no will is published, since the client is reconnecting and not lost) and starts a
  new one for the new CONNECT: will handshake if the Will flag is set, a new broker connection with the new
  duration, and CONNACK. The client does not receive a DISCONNECT for the old session.
- The slot of the old session is reused, so `max_clients` does not count both, and the client is not rejected with
  Congestion when the limit is reached.
- A CONNECT from a sleeping client keeps its existing wake-up meaning (CONNACK and flush); this intent does not
  change it.
- A CONNECT during the will handshake or while the broker is being dialled keeps being ignored (a retransmission of
  the CONNECT that started the session).
- Logged with client id; never will contents.

### Oversize downlink (D11, 6.10)
- A broker PUBLISH whose MQTT-SN datagram cannot be sent (it exceeds what one UDP datagram can carry, 65 507 bytes
  of payload for IPv4) is dropped and logged, and a QoS 1 one is acknowledged to the broker so it does not retry.
  The same goes for a REGISTER whose datagram is too large, which drops the messages held for it and forgets the
  topic name.
- Nothing about that message enters the retry set, so the client is not retransmitted to, not declared lost, and
  not disconnected, and its will is not published. Later normal messages are delivered.
- The limit also applies to messages flushed after a wake-up.
- Logged with sizes, never payload or topic bytes.

## Spec changes
- Changes: sleep / Sleep entry and expiry (re-sleep with a new duration)
- Changes: sleep / Wake-up flush (re-sleep during a flush)
- Adds: transport-and-sessions / Reconnect from the same address
- Changes: topics-and-pubsub / Downlink publish (oversize messages)
- Changes: sleep and transport-and-sessions / Not supported (remove the matching lines if any)
- Changes: CONFORMANCE.md (D1, D3 and D11 move to "fixed"; summary table updated)

## Out of scope
- Every other finding in `CONFORMANCE.md`, including D4 (CONNECT while asleep) and the error signalling of D7.
- Applying a different keep-alive or will on a CONNECT from a sleeping client.
- Limits for uplink message size.

## Decisions taken
- A repeated CONNECT ends the old session with a plain MQTT DISCONNECT, so no will is published.
- A DISCONNECT with a duration during a flush keeps the unsent messages buffered.
