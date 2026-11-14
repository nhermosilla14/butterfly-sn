# Sleep state, buffering and wake-up flush

- Status: Accepted
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: sleep.md, transport-and-sessions.md, topics-and-pubsub.md

## Context
A sleeping MQTT-SN client keeps its session and broker connection but stops sending. The gateway must buffer
downlink messages, deliver them when the client wakes, and not lose or duplicate broker acknowledgements. The
legacy gateway acknowledged QoS 1 at buffering time, capped the queue at 20, dropped the newest, and never sent
PINGRESP when the flush needed no REGACK. The current gateway has one serialized session loop, a bounded
downlink hold and REGISTER machinery, and timers for keepalive and inactivity.

## Decision
- Session state gains a sleeping flag, the sleep duration, a sleep timer, a bounded FIFO buffer and a wake mode.
  Only the session goroutine touches them.
- DISCONNECT with duration > 0 from an active session: reply DISCONNECT, keep the broker connection open, stop
  the active inactivity timer and start a sleep timer at 1.5 times the duration. The broker keepalive timer keeps
  running.
- While asleep, a broker PUBLISH is appended to the buffer (max 20, newest dropped and logged when full). QoS 1
  is acknowledged to the broker when buffered; QoS 0 needs no ack. Nothing is sent to the client. A broker packet
  already acked may be lost on overflow, by design.
- Wake triggers:
  - PINGREQ with an empty buffer: PINGRESP at once, return to asleep, restart the sleep timer.
  - PINGREQ with buffered messages: enter awake and flush; PINGRESP after every buffered PUBLISH frame has been
    written; then return to asleep and restart the sleep timer.
  - CONNECT: immediately CONNACK Accepted, leave sleep permanently, flush the same way and stay active. The
    CONNECT duration, clean-session flag and will are ignored for this transition, matching the legacy gateway.
- Flush: each buffered message is resolved in order. Known table topics use their id; tracked short
  subscriptions use the short form; unknown topics allocate an id, send REGISTER and wait for REGACK through the
  existing hold machinery. A rejected REGACK drops the entry. PINGRESP is delayed while a REGISTER for a buffered
  message is outstanding; a missing REGACK waits indefinitely until the retransmission intent bounds it.
- Buffered QoS 1 messages keep their original broker packet id as msg id. Their client PUBACK is matched against
  a pre-acknowledged set and never produces a second MQTT PUBACK; an unknown or rejected id is logged.
- DISCONNECT from asleep or awake is a normal disconnect: MQTT DISCONNECT, client DISCONNECT, session removed and
  buffer dropped. Sleep timer expiry is a loss: remove the session, close the broker socket bare when a will
  exists (publishing it), otherwise send MQTT DISCONNECT, send a best-effort client DISCONNECT and drop the
  buffer.
- Any packet from an asleep client other than PINGREQ, CONNECT or DISCONNECT is ignored and logged, except the
  REGACK and PUBACK that belong to a running flush.

## Alternatives considered
- Delay the QoS 1 broker ack until delivery — more reliable, but the broker's in-flight window would stall while
  the client sleeps and the legacy behaviour is acked-at-buffering; rejected for now.
- Delay CONNACK until the flush ends — the spec's CONNECT flow expects CONNACK for the new active session, and
  the legacy sent it first.
- Drop the buffer on PINGREQ if no REGACK arrives — that would silently lose messages; waiting matches legacy and
  leaves retransmission to its own intent.
- Unbounded buffer or byte-size cap — 20 messages matches legacy and keeps memory bounded.

## Consequences
Sleeping clients no longer lose the broker session and can receive messages on wake. QoS 1 buffered messages can
be acked and then lost on a full buffer, which is visible in logs and documented. PINGRESP is now reliable after a
flush, fixing the legacy bug, but a lost REGACK stalls the wake until the retransmission work adds timers. The
buffer and pre-acknowledged map are small and per session, so 100 clients stay bounded. Will publication on sleep
expiry reuses the existing teardown.
