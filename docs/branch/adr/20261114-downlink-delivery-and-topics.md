# Downlink delivery, topic ids and bounds

- Status: Accepted
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: topics-and-pubsub.md

## Context
Each client has its own broker connection and its own MQTT-SN topic table, so a subscription is a broker
subscription on that connection and a downlink topic must be translated to the client's topic id. The legacy
gateway registered unknown topics with the client, held the PUBLISH until REGACK, copied the broker packet id as
the MQTT-SN msg id, and acknowledged QoS 1 to the broker after the client did. It also had an unbounded table
bug and short-topic special cases that read past one-character names.

## Decision
- The per-client topic table is capped at 50 names. Client REGISTER or SUBSCRIBE that would add a 51st name
  answers REGACK/SUBACK Congestion. The cap is a constant, matching the legacy gateway; no configuration key.
- SUBSCRIBE and UNSUBSCRIBE msg ids are passed unchanged as MQTT packet ids; SUBACK and UNSUBACK copy the ids
  back.
- SUBACK topic id:
  - exact normal topic name: the id allocated in the client's table (registering it if new);
  - wildcard filter: 0x0000;
  - short topic name: the two bytes of the name as a 16-bit value.
  - Broker refusal (0x80) maps to return code 0x02 with QoS 0 and topic id 0.
- UNSUBSCRIBE removes the broker subscription but keeps the client's topic table entry, as MQTT-SN topic ids
  remain valid for publishing.
- Broker PUBLISH delivery to an active client:
  1. If the topic exactly matches a short subscription, send a short PUBLISH with the two name bytes, copying
     QoS, retain and DUP, and no REGISTER.
  2. Else if the topic is in the table, send a PUBLISH with its id.
  3. Else allocate an id, send a gateway REGISTER with a per-session msg id (1..65535, never 0), and hold the
     PUBLISH until REGACK. Accepted releases the held PUBLISH. Rejected drops it, acknowledges QoS 1 to the
     broker and removes the name from the table, so a later broker publish registers it again with a fresh id
     instead of reusing an id the client refused. An unknown REGACK id is logged and ignored.
  4. If the table is full, drop the PUBLISH, log, and for QoS 1 acknowledge the broker.
- Holds are bounded per client (20 messages). Overflow drops the message and, for QoS 1, acknowledges the broker.
  A second publish for a topic already awaiting REGACK joins the same hold.
- Topic ids are allocated monotonically and never reused after a name is removed, while at most 50 names are
  live at once.
- A QoS 1 PUBLISH whose broker packet id is still awaiting the client's PUBACK is dropped and logged without
  acknowledging the broker: that id belongs to the in-flight message, and acknowledging it would acknowledge
  the wrong delivery.
- Downlink QoS 1 PUBLISH uses the broker packet id as the MQTT-SN msg id. The gateway sends MQTT PUBACK only
  after the client's PUBACK Accepted, which is the same ordering rule as uplink. A rejected PUBACK drops the
  pending entry, logs and still acknowledges the broker so it does not retry forever.
- A broker PUBLISH with an empty topic, wildcard characters, QoS 2, a missing id or a malformed body is a
  protocol error and ends the broker connection through the existing loss path. One- or two-character topic
  names use the short rule only when a matching short subscription exists; otherwise they register normally,
  which fixes the legacy out-of-bounds bug.
- Topic names, filters and payloads are never logged.

## Alternatives considered
- No table cap, relying on 16-bit ids — one client could allocate tens of thousands of names; rejected for
  bounded resources.
- Send every short topic name as short, as the legacy gateway did — ambiguous for wildcard subscriptions and
  unsafe for 0/1-character names; rejected.
- Acknowledge QoS 1 to the broker on receipt and hold delivery — that is the sleep behaviour; for active
  clients it would hide delivery failures, so the ack waits for the client.
- Drop rejected client PUBACKs without acknowledging the broker — leaves the broker retrying forever; rejected.
- Configurable table and hold limits — more surface without a current need; the constants match legacy.

## Consequences
Downlink uses the same id pass-through and ack ordering as uplink, so the gateway stays a translator. Unknown
topics cost one REGISTER round trip and a bounded hold. Full tables now fail predictably instead of crashing.
Retransmission of REGISTER/PUBLISH towards the client is still absent, so a lost REGACK holds the message until
the session ends; the later retransmission intent adds timers. Sleep builds on the same buffer/hold machinery but
with its own policy.
