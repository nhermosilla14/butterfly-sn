# Sleep, buffering and wake-up flush

- Status: In progress
- Date: 2026-11-14
- Owner: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Depends on: none

## Goal
A battery-powered client can enter sleep without dropping its broker connection. The gateway keeps the connection
alive, buffers downlink messages within a fixed bound, delivers them in order when the client wakes with PINGREQ
or CONNECT, and sends PINGRESP only after the flush. A client that never wakes is declared lost, its will is
published and its slot is freed.

## Acceptance criteria
- DISCONNECT with a duration greater than 0 puts the session to sleep: the client gets DISCONNECT, the broker
  connection stays open, and the active inactivity timer is replaced by a sleep timer of 1.5 times the sleep
  duration.
- While asleep the client is not timed out by its CONNECT keepalive, and broker keepalive pings continue.
- Broker PUBLISH messages for an asleep client are buffered instead of delivered. QoS 1 is acknowledged to the
  broker at buffering time; QoS 0 needs no ack. The buffer holds at most 20 messages, dropping the newest on
  overflow and logging. A dropped QoS 1 message is already acknowledged and therefore lost.
- PINGREQ from an asleep client wakes it: buffered messages are delivered in arrival order, topics not in the
  table are registered first and delivered after REGACK, and PINGRESP is sent only after every buffered PUBLISH
  has been written. The client then returns to sleep and the sleep timer restarts. An empty buffer gets PINGRESP
  immediately.
- CONNECT from an asleep client gets CONNACK Accepted, flushes the same way, and leaves the session active; no
  new broker connection is opened and the new CONNECT duration, clean-session flag and will are ignored.
- Buffered QoS 1 messages are sent with a gateway-assigned msg id, because the broker may reuse its packet id
  after the early acknowledgement. The client PUBACK is accepted but no second MQTT PUBACK is sent to the broker.
- DISCONNECT from an asleep or awake client ends the session normally: MQTT DISCONNECT to the broker, DISCONNECT
  to the client, buffer dropped and no will published.
- Sleep timer expiry declares the client lost: the broker connection is closed so a will is published when one
  exists, the client gets a best-effort DISCONNECT, the buffer is dropped and the slot is freed.
- Broker loss or gateway shutdown while asleep follows the existing broker-loss and shutdown paths; buffered
  messages are discarded.
- Any other packet from an asleep client is ignored and logged, except REGACK and client PUBACK while a wake
  flush is running.
- No new configuration keys and no new dependencies.

## Spec changes
- Adds: sleep / Sleep entry and expiry
- Adds: sleep / Buffering
- Adds: sleep / Wake-up flush
- Changes: transport-and-sessions / Connect and disconnect
- Changes: transport-and-sessions / Keepalive and liveness
- Changes: transport-and-sessions / Not supported
- Changes: topics-and-pubsub / Downlink publish

## Out of scope
- Retransmission of REGISTER, PUBLISH or PINGREQ; a missing REGACK keeps the flush waiting, as in the legacy
  gateway.
- Persistent sessions (CleanSession=0), will persistence and session resumption.
- Multiple sleep durations, sleep queue ageing or byte-size caps.
- QoS 2, predefined topics, discovery, will updates.

## Decisions taken
- Sleep expiry uses 1.5 times the sleep duration, mirroring the active keepalive tolerance.
- The buffer holds 20 messages per client and drops the newest on overflow, matching the legacy gateway; QoS 1 is
  acknowledged to the broker when buffered, so overflow means loss.
- PINGRESP always follows the flush, fixing the legacy held-PINGREQ bug; when a REGACK is missing, PINGRESP
  waits, and the retransmission intent will bound it.
- CONNECT from sleep sends CONNACK immediately and then flushes, rather than delaying CONNACK until after the
  buffer.
- Buffered QoS 1 messages are never acknowledged to the broker twice.
