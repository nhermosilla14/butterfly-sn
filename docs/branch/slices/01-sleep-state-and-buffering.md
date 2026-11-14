# Sleep state and buffering

- Status: Done
- Intents: 20261114-sleep-and-buffering
- ADRs: 20261114-sleep-state-and-buffer-flush
- Depends on: none

## Scope
A client can enter sleep with DISCONNECT and a duration. The broker connection stays open, the keepalive timer
keeps running, the active inactivity timer is replaced by the sleep timer, and downlink messages are buffered
instead of delivered. A client that never wakes is declared lost, its will is published and its slot is freed.

## Layers touched
- Services: session sleeping state, sleep timer, bounded buffer, buffering path in downlink delivery, sleep
  expiry teardown, DISCONNECT semantics from asleep.
- Interface: none; the wire uses the existing DISCONNECT duration and PINGREQ.

## End-to-end test
- A client with a will sleeps with a 1 s duration; while asleep it gets no broker deliveries and is not timed out
  by its CONNECT keepalive; after 1.5 s the sleep timer expires, the client gets DISCONNECT, the observer
  receives the will and a second client takes the freed slot.
- DISCONNECT from asleep ends the session with no will and frees the slot.
- Covers criteria: sleep entry, no active timeout, buffering side effects, expiry, slot, will, explicit
  disconnect.

## Out of scope
- Waking and flushing the buffer; next slice.
- CONNECT from asleep, REGISTER during a flush, PINGRESP after flush.
- Retransmission, persistence, QoS 2.
