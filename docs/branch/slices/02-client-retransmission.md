# Retransmission towards the client

- Intents: 20261114-broker-outage-inflight-cleanup
- ADRs: 20261114-gateway-retry-and-pending-timeouts
- Depends on: 01

## Scope
Gateway REGISTER and downlink QoS 1 PUBLISH are supervised. They are retransmitted every retry interval (PUBLISH
with DUP, same msg id), and when the limit is exhausted the client gets DISCONNECT, the session is torn down with
the will-aware client-loss teardown and nothing is acknowledged to the broker. Replies stop the retries. Wake-up
flush REGISTER and PUBLISH use the same rule. A sleeping client is never retransmitted to, and timers restart from
the wake-up.

## Layers touched
- Foundation: a per-session retry set and one session timer in `internal/gateway`.
- Services: hooks where REGISTER and PUBLISH are sent and where REGACK and PUBACK arrive; sleep entry clears the
  set; logs with client id, type and attempt.
- Interface: the MQTT-SN datagrams seen by a test client.

## End-to-end test
- Gateway tests with a real broker connection and a short interval: a silent client sees the REGISTER repeated
  and then DISCONNECT, the slot is free and a will is published; a client that answers the second REGISTER gets
  the held PUBLISH and no further retries; a QoS 1 PUBLISH is repeated with DUP and the same msg id; a flush with
  a missing REGACK ends in DISCONNECT instead of waiting; a sleeping client receives nothing and is not timed out
  by the retry timer; a reply during the last interval stops the sequence.
- Covers criteria: REGISTER and PUBLISH retransmission and give-up, flush bounded waits, no timers while asleep,
  no duplicate for a client that answers in time, logging without payloads.

## Out of scope
- Broker-side requests (slice 03).
