# Downlink pub/sub: subscriptions and broker-to-client publishes

- Status: In progress
- Date: 2026-11-14
- Owner: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Depends on: none

## Goal
An MQTT-SN client can subscribe and unsubscribe, including wildcard filters and short topic names, and receives
messages published to the broker by other clients. Topics the client never registered are announced with a
gateway REGISTER before the PUBLISH. Downlink QoS 1 is acknowledged to the broker only after the client
acknowledged it, and per-client topic state stays bounded.

## Acceptance criteria
- SUBSCRIBE of a normal topic name gets SUBACK Accepted with the granted QoS and a topic id that later downlink
  PUBLISH messages use. The broker holds the corresponding subscription.
- SUBSCRIBE of a wildcard filter gets SUBACK Accepted with topic id 0x0000 and the broker holds the filter.
- UNSUBSCRIBE of a normal or wildcard filter gets UNSUBACK with the client's msg id, and later publishes to that
  filter are not delivered.
- A broker PUBLISH for a topic already in the client's table arrives as one MQTT-SN PUBLISH with that topic id,
  preserving payload bytes, QoS, retain and DUP; no REGISTER precedes it.
- A broker PUBLISH for a topic the client has not registered causes one gateway REGISTER, and only after REGACK
  Accepted the PUBLISH is delivered with the new topic id.
- A downlink QoS 1 PUBLISH reaches the client with the broker's packet id as msg id; the gateway sends MQTT
  PUBACK to the broker only after the client PUBACK Accepted. A rejected client PUBACK is logged and the broker
  is still acknowledged, so the message is not retried forever.
- A two-character short topic name works in client PUBLISH, SUBSCRIBE and UNSUBSCRIBE, and downlink messages for
  a short subscription arrive as short PUBLISH without REGISTER.
- The per-client topic table holds at most 50 names. A client REGISTER or SUBSCRIBE that would exceed it gets
  Congestion. A gateway registration caused by a broker PUBLISH at a full table drops that message, acknowledges
  QoS 1 to the broker and logs; nothing crashes or grows without bound.
- Empty or one-character broker topic names are handled safely: empty names are dropped; short ones use normal
  registration unless a matching short subscription exists.
- Broker packets of unsupported type or with QoS 2 are logged and end the broker connection through the existing
  broker-loss path; they never reach the client unvalidated.
- Topic names, filters and payloads are never logged.

## Spec changes
- Adds: topics-and-pubsub / Subscribe and unsubscribe
- Adds: topics-and-pubsub / Downlink publish
- Adds: topics-and-pubsub / Short topic names
- Changes: topics-and-pubsub / Register topic (50-name bound)
- Changes: topics-and-pubsub / Not supported
- Changes: configuration / Not supported (no new keys)

## Out of scope
- Sleep buffering; a later intent delivers to asleep clients and flushes on PINGREQ.
- Retransmission and DUP handling towards the client for unanswered REGISTER or PUBLISH.
- QoS 2, predefined topic ids, broker TLS/auth, will updates, discovery.
- Session takeover/rebinding, message size caps and load hardening.

## Decisions taken
- SUBSCRIBE, UNSUBSCRIBE, SUBACK and UNSUBACK msg ids pass through unchanged as MQTT packet ids, matching the
  existing QoS 1 publish rule.
- The topic table limit is a fixed 50 names, matching the legacy gateway, with no new configuration key.
- Per-client held PUBLISH messages waiting for REGACK are bounded; overflow drops the message and acknowledges
  QoS 1 to the broker.
- Short topic names are exactly two characters; other lengths are rejected with Congestion.
- No gateway-side message deduplication: a broker retransmission may be delivered again, as in the legacy
  gateway. Retransmission policy is a separate intent.
