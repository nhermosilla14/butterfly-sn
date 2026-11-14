# Short topic names

- Status: Done
- Intents: 20261114-downlink-pubsub
- ADRs: 20261114-downlink-delivery-and-topics
- Depends on: 02

## Scope
Two-character short topic names work in both directions: a client can publish, subscribe and unsubscribe using
the short type, and a downlink PUBLISH for an exact short subscription arrives as a short PUBLISH without a
REGISTER. Other short lengths are refused, and empty or one-character broker topics are handled safely through
normal registration.

## Layers touched
- Services: session topic-id handling for the short type in PUBLISH, SUBSCRIBE and UNSUBSCRIBE; short
  subscription tracking; downlink short delivery.
- Interface: none.

## End-to-end test
- PUBLISH with a normal topic id and PUBLISH with a short topic id reach the same broker topic with the same
  payload.
- SUBSCRIBE short gets SUBACK with the two name bytes as topic id, and a broker publish to that name arrives as
  a short PUBLISH with no REGISTER.
- A short name of the wrong length is refused with Congestion.
- A one-character broker topic is delivered via normal REGISTER, not a short PUBLISH.
- The short-subscription set is capped at 50, and wildcard or empty topic names never reach a broker write.
- Covers criteria: short publish/subscribe/unsubscribe, downlink short delivery, safe short lengths.

## Out of scope
- Predefined topic ids (project non-goal), sleep, retransmission, QoS 2.
