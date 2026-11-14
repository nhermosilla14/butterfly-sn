# Gateway retry timers and bounded waits for pending exchanges

- Status: Accepted
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: topics-and-pubsub.md, sleep.md, transport-and-sessions.md, configuration.md

## Context
The gateway starts exchanges it never supervises. A REGISTER or QoS 1 PUBLISH sent to the client waits for the
REGACK or PUBACK forever, and a wake-up flush waits with it. A SUBSCRIBE or UNSUBSCRIBE forwarded to the broker
waits for the answer forever, and the entries stay in the session maps until the session ends. MQTT-SN 1.2
defines a retry timer (Tretry) and counter (Nretry) for the client only (6.13, 7.2). For gateway-originated
messages it defines no retransmission or timeout (6.10 only says the gateway waits for the REGACK), and nothing
covers a broker that stops answering. Where the specification is silent, the project rule is that the session
ends. Both peers can stall: a silent client is covered by the keep-alive timer only when it sends nothing at all,
and a silent broker only by the ping timeout, which does not look at individual requests.

## Decision
Supervision lives in the session goroutine, like the existing keep-alive, inactivity and sleep timers. No new
goroutine is created.

Configuration: `retry_interval` (seconds, default 10, must be greater than 0) and `retry_limit` (number of
retransmissions, default 5, must be greater than 0). Invalid values fail startup naming the key. The gateway
converts the interval to a duration it keeps in a field, so tests can shorten it without changing the config unit.

Towards the client (REGISTER, QoS 1 PUBLISH):
- Each message sent is recorded in a per-session retry set with the exact datagram, its attempt count and its
  due time, keyed by message type and msg id. The set is bounded by the existing limits (20 pending registers
  through the hold limit, 20 pending QoS 1 PUBLISHes, 20 pre-acknowledged flushed PUBLISHes).
- One timer, armed for the earliest due time (a nil channel when the set is empty), drives retransmission. On
  expiry each due entry is sent again unchanged, except that a PUBLISH gets DUP set. The due time becomes now plus
  `retry_interval`.
- An entry is removed when its reply arrives: REGACK (any return code), client PUBACK for pending and
  pre-acknowledged PUBLISHes.
- When an entry would need more than `retry_limit` retransmissions, the client is declared lost: the session ends
  through the existing client-loss teardown (session removed, broker connection ended with the will kept for the
  broker to publish or MQTT DISCONNECT otherwise, client sent a best-effort DISCONNECT). Nothing is acknowledged
  to the broker during that teardown; held and pending messages are discarded with the session.
- While the client sleeps, the retry set is empty or paused: entries are dropped when the session goes to sleep
  (held messages already move to the buffer) and a flush creates new ones, so a sleeping client is never
  retransmitted to. Timers restart from the wake-up, never from before the sleep.

Towards the broker (SUBSCRIBE, UNSUBSCRIBE):
- Each pending request records its send time. A request still unanswered after `retry_interval` times
  `retry_limit` means the broker is not responding: the session ends through the existing broker-loss teardown
  (DISCONNECT to the client). There is no retransmission, because MQTT over TCP does not retransmit.
- The same single session timer covers these deadlines.

Pending state lives in session-owned maps, so ending the session for any reason releases it; no cleanup runs
after a broker loss, and a CONNECT with the same client id gets a new session with empty maps.

Logging: each retransmission, give-up and cleanup logs client id, message type and attempt, never payload bytes.

## Alternatives considered
- One timer per pending exchange (`time.AfterFunc`) — callbacks run outside the session goroutine and need locks
  around state that is currently confined to it.
- A separate retry goroutine — same concurrency cost and an extra goroutine to join at shutdown.
- Drop only the held messages when REGISTER retries run out and keep the session — leaves a client that does not
  answer in the active state; the spec offers no basis for it and the keep-alive may be 0.
- Answer SUBACK or UNSUBACK with Congestion on a broker timeout and keep the session — a broker that ignores a
  request is not healthy; the spec is silent, so the session ends.
- Fixed Tretry and Nretry with no configuration — the spec gives ranges, not values, and deployments differ in
  link quality.
- Retransmit uplink PUBLISH to the broker — the client owns that retry (6.13), and TCP does not lose packets.

## Consequences
A stalled client or broker is detected within about `retry_interval` times `retry_limit` (50 s by default)
without waiting for the keep-alive. Messages in flight when the retries run out are lost with the session. The
retry set stores sent datagrams, so memory per session grows by at most the existing bounds. Tests use a short
interval set on the gateway. Changing the retry semantics later (for example backoff) changes only the timer
callback.
