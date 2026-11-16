# MQTT-SN v1.2 conformance

How closely butterfly-sn follows the MQTT-SN v1.2 specification (v1.2, 2013), where it
deliberately or accidentally departs from it, and which features it does not offer. Read this before putting a
client on the gateway that you did not test.

- Reviewed: 2026-11-14, `develop` at commit `9621695`. D1, D3 and D11 were fixed afterwards (see below).
- Gateway type: transparent gateway (section 4.1), UDP over IPv4 on the MQTT-SN side, MQTT 3.1.1 over plain TCP on
  the broker side, one broker connection per client.
- Scope of the review: the whole specification (sections 3 to 7). Each message, field, procedure and recommended
  value was classified as Compliant, Deviates, Not supported or Not applicable (client-only duties).

## How this was checked, and its limits
Three independent reviewers read the specification against the code (`internal/mqttsn`, `internal/gateway`,
`internal/config`), the behaviour documents in `docs/spec/` and the existing tests. Two findings were then
re-checked by hand against the code (a repeated CONNECT, and an oversize downlink message). This is a static
review: it did not run an external conformance suite or real devices, so it can miss defects that only a live
interoperability test shows. Items marked **unverified** could not be settled by reading the code.
The behaviour that is delivered and proven by tests is described in [docs/spec/](docs/spec/README.md); this file
compares that behaviour with the standard.

## Summary

| Area | Verdict |
|---|---|
| Message formats: header and 3-byte lengths, message types, flag bits, field order and byte order | Compliant |
| Discovery: ADVERTISE, SEARCHGW, GWINFO formats and timers | Compliant, with minor departures (D17) |
| Connection setup and will handshake (WILLTOPICREQ, WILLTOPIC, WILLMSGREQ, WILLMSG) | Compliant for QoS 0 and 1; departures D4, D14 (D3 fixed) |
| CleanSession | **Not honoured** (D5) |
| Will update (WILLTOPICUPD, WILLMSGUPD) | **Rejected on purpose** (D6) |
| Topic registration, PUBLISH and PUBACK for QoS 0 and 1, short topic names | Compliant for the happy path; error signalling departs (D7, D8, D9) |
| SUBSCRIBE and UNSUBSCRIBE, wildcards | Compliant, except QoS 2 requests (D12) |
| Gateway-originated REGISTER and PUBLISH, retransmission with DUP | Compliant, with the departure D19 (D11 fixed) |
| Keep-alive and PING | Compliant, with a different timer tolerance (D2) |
| Sleeping clients | Mostly compliant: buffer limits (D10), CONNECT while asleep (D4); re-sleeping fixed (D1) |
| QoS 2, QoS -1, predefined topic ids, forwarder encapsulation, aggregating gateway | **Not supported** (see "Features that are missing") |

## Features that are missing
These are known and, except where stated, deliberate project non-goals (see [docs/vision.md](docs/vision.md)).
A client that needs one of them cannot use this gateway for that feature.

| Feature | Spec § | Status | What a client sees |
|---|---|---|---|
| QoS 2 publish and subscribe (PUBREC, PUBREL, PUBCOMP) | 5.4.12, 5.4.14 | Not supported, planned for a later release | A QoS 2 PUBLISH gets no reply. A QoS 2 SUBSCRIBE is refused with return code 0x03 (D12). A will with QoS 2 gets CONNACK 0x03. |
| QoS -1 (publish without a connection) | 6.8, 7.1 | Not supported, non-goal | The PUBLISH is ignored; from an unknown address it gets DISCONNECT. |
| Predefined topic ids | 5.3.4, 6.7 | Not supported, non-goal | PUBLISH is ignored; SUBSCRIBE gets return code 0x03. |
| Forwarder encapsulation (message type 0xFE) | 5.5 | Not supported, non-goal | Clients behind a forwarder cannot connect. |
| Aggregating gateway | 4.2 | Not supported, non-goal | Not applicable to a transparent gateway. |
| Stand-by gateway and ADVERTISE monitoring | 6.1 | Not supported, optional in the spec | None. |
| Gateway-originated PINGREQ towards clients | 6.11 | Not supported, optional in the spec | Client supervision is passive. |
| Persistent sessions and CleanSession=0 | 6.3 | Not supported (D5) | Subscriptions and will are lost on every disconnect. |
| Will updates | 6.4 | Not supported, rejected with 0x03 (D6) | WILLTOPICRESP or WILLMSGRESP with 0x03. |
| Multicast, DTLS, UDP over IPv6, ZigBee, XBee, LoRa, RFCOMM transports | 7.4 | Not supported, non-goals | Only UDP over IPv4 is offered. |
| Authentication and TLS towards the broker | n/a | Not supported, non-goal | The broker connection is plain TCP without credentials. |

## Where the gateway deviates from the specification
Severity describes the risk for a standards-compliant client: **high** can strand a client or lose its session,
**medium** causes silent loss or a long stall, **low** is a cosmetic or edge-case departure. "By design" means a
documented decision; the rest are defects or gaps that no decision covers yet.

### High
**D1. FIXED. DISCONNECT with a duration while asleep or awake ended the session (6.14, 5.4.21).** A DISCONNECT with a duration now changes the sleep duration and keeps the client asleep, also in the middle of a wake-up flush; see `docs/spec/sleep.md`. What it was: the
specification lets a client change its sleep duration, and return to sleep after a wake, with a new DISCONNECT that
carries a duration. The gateway treats it as a full disconnect: the broker connection is closed, the session is
removed and the client gets a plain DISCONNECT. The client believes it is asleep but has no session, so its next
PINGREQ gets DISCONNECT, and its subscriptions and buffered messages are gone. The behaviour is documented in
`docs/spec/sleep.md` and encoded in a test, but it contradicts the standard.

### Medium
**D2. Keep-alive and sleep tolerance (7.2), by design.** The standard asks for 10% above the indicated duration when
it is longer than one minute and 50% above when it is one minute or less. The gateway uses a flat 1.5 times for both
the keep-alive and the sleep timer. For a 300 s keep-alive it waits 450 s instead of 330 s before declaring the
client lost and letting the broker publish its will. It never acts earlier than the standard allows. Up to 60 s the
two agree.

**D3. FIXED. CONNECT from an address that already had a live session was ignored (6.2).** The new CONNECT now replaces the old session (no will is published); see `docs/spec/transport-and-sessions.md`. What it was: No CONNACK is sent. A
client that reboots and reconnects from the same address and port gets no answer until its old session times out
(1.5 times the keep-alive), and never when the keep-alive is 0. Confirmed in the code.

**D4. CONNECT while asleep ignores its content (6.14), a defect.** The client gets CONNACK and a flush, but the new
duration, the Will flag and the CleanSession flag are ignored. A client that connects again asking for a will
waits for a WILLTOPICREQ that never comes.

**D5. CleanSession is ignored (3.5, 5.3.4, 6.3, 6.12), by design (planned).** The broker session is always clean,
and CONNACK still reports "accepted". A client that connects with CleanSession=0 expecting its subscriptions or
queued messages to persist silently loses them. The gateway also never sends a REGISTER for topics of a returning
client (6.5).

**D6. Will updates are rejected (6.4, 5.4.22 to 5.4.25), by design.** MQTT 3.1.1 fixes the will when the
connection opens, so WILLTOPICUPD and WILLMSGUPD get return code 0x03, which the standard allows as a rejection
reason. Consequence for a client: an empty WILLTOPICUPD, meant to delete the will, does not delete it, so a will
the client wanted removed is still published if the client is lost.

**D7. Errors on the uplink are not signalled (6.6, 5.4.13).** A PUBLISH with an unknown topic id, an invalid name,
QoS 2, a predefined id, or message id 0 is dropped silently. The standard expects PUBACK with 0x02 "invalid topic
id" so that the client registers the topic again; the gateway never sends PUBACK with an error code. A QoS 1
client retransmits and finally declares the gateway lost.

**D8. A client can only have 50 topic names, and they are never freed (6.5, 6.10).** A wildcard subscription such
as `sensors/#` registers each new concrete topic in the session's table. After 50 distinct topics, further
messages are dropped (QoS 1 ones are still acknowledged to the broker, so they are lost silently) and REGISTER or
SUBSCRIBE get return code 0x01.

**D9. A client PUBACK with 0x02 does not correct the topic mapping (6.10).** It is only logged; the stale id stays
in the table and later messages keep using it.

**D10. The sleep buffer is small and acknowledges early (6.14), by design.** It holds 20 messages and drops the
newest beyond that, and QoS 1 messages are acknowledged to the broker when they are buffered. A message that is
dropped, or lost with the gateway, therefore looks delivered to the broker.

**D11. FIXED. Oversize downlink messages ended up disconnecting the client (6.10).** They are now dropped silently as the standard says and the client stays connected; see `docs/spec/topics-and-pubsub.md`. What it was: a message too large to encode
or send (more than 65 535 bytes, or more than a UDP datagram allows) is not sent, but its REGISTER or QoS 1
PUBLISH stays in the retry set. After the retries the client is declared lost, disconnected, and its will fires.
The standard says the gateway silently aborts such a publication. Confirmed in the code.

**D12. SUBSCRIBE with QoS 2 is refused (5.4.15, 5.4.16), by design (planned).** The standard lets the gateway grant
a lower QoS in SUBACK. The gateway answers 0x03 instead, so a client that always subscribes at QoS 2 fails
entirely.

**D13. Sessions are keyed by the UDP source address (6.2).** A client whose source port changes (a NAT rebinding,
or a reboot) is seen as unknown and gets DISCONNECT; a sleeping client loses its buffered messages that way.

### Low
**D14. The will handshake is not retransmitted (6.2, 6.13).** WILLTOPICREQ and WILLMSGREQ are sent once, a repeated
CONNECT or WILLTOPIC from the client is ignored, and the whole handshake has a fixed 10 s timeout that ends with
DISCONNECT. On a lossy link one lost packet fails the connect.

**D15. Return codes are coarse (5.3.10, 5.4.5).** Every broker failure (dial error, refusal, bad will topic) is
reported as CONNACK 0x01 "congestion". An empty client id gets DISCONNECT instead of a CONNACK; a CONNECT with a bad
protocol id is dropped; a client id longer than 23 characters is accepted. A SUBSCRIBE with a wrong-length short
name gets 0x01, which tells a compliant client to wait and retry, and a broker-refused SUBSCRIBE gets 0x02.

**D16. Unknown or unsupported message types from an unknown address get DISCONNECT (5.4.21).** This includes QoS 2
acknowledgements and encapsulated frames.

**D17. Discovery (5.4.1 to 5.4.3, 6.1).** GWINFO is sent by unicast, not broadcast, and the radius is ignored; no
pending GWINFO is suppressed when another gateway answers; the first ADVERTISE is sent only after a full interval
(default 900 s, exactly 15 minutes where the standard says "greater than 15 minutes"); ADVERTISE continues while the
broker is unreachable; SEARCHGW replies are not rate limited.

**D18. Small protocol details.** A PINGREQ client id is decoded but not compared with the session. SUBSCRIBE,
UNSUBSCRIBE and QoS 1 PUBLISH with message id 0, and UNSUBSCRIBE beyond 20 pending requests, get no reply. Packets
from a client that is asleep and has not woken are ignored without a reply. Uplink message ids for PUBLISH,
SUBSCRIBE and UNSUBSCRIBE share one packet id space towards the broker, so equal ids in flight could collide.

**D19. The gateway can have many messages in flight (6.10).** The standard limits the client to one outstanding
REGISTER, SUBSCRIBE or QoS 1 PUBLISH; the gateway may have up to 20 REGISTERs and 20 QoS 1 PUBLISHes outstanding
towards one client, which can overload a constrained device. A client's REGACK rejection of a gateway REGISTER
does not unsubscribe the topic (6.10), so the next message registers it again.

### Gateway retransmission is by analogy
The standard defines the retry timer and counter (section 6.13, values in 7.2) only for the client side, and gives
no rule for a gateway whose REGISTER or PUBLISH goes unanswered. The gateway applies the same procedure
(`retry_interval` 10 s and `retry_limit` 5 by default, with DUP set on a retransmitted PUBLISH) and, when it is
exhausted, treats the client as lost. An unanswered SUBSCRIBE or UNSUBSCRIBE towards the broker ends the session
after `retry_interval` times `retry_limit`. These are project decisions where the specification is silent, not
requirements of the standard. See the ADR `20261114-gateway-retry-and-pending-timeouts`.

## Unverified
- Whether the default broadcast address (`255.255.255.255`) reaches every interface on a multi-homed host.
- A PINGREQ that arrives while a wake-up flush is already running (it appears to restart the flush).
- A retained broker message that arrives before the SUBACK has been processed, and a short-name subscription that
  contains a wildcard character.
- The behaviour of a broker that delivers QoS 2 to a QoS 1 subscription.

## Compliant, for the record
- All message formats checked: both length forms, message types 0x00 to 0x1D, the flag bits, big-endian fields,
  return code values, topic id ranges (1 to 0xFFFE, 0 and 0xFFFF never allocated) and short topic names.
- The will handshake order, empty WILLTOPIC meaning no will, will publication by the broker only for abnormal
  ends, and a clean DISCONNECT publishing nothing.
- Per-client topic tables (7.3), wildcard SUBSCRIBE answering topic id 0 and REGISTER on the first matching message,
  REGISTER before PUBLISH on the way down, QoS 1 acknowledgement ordering in both directions.
- Keep-alive supervision of the client with the will activated on loss, PINGREQ and PINGRESP, the asleep and awake
  states, buffering, flush ending with PINGRESP, and the sleep timer.
- Retry values within the recommended ranges of 7.2 (Tretry 10 s, Nretry 5).

## Closing the gaps
D1, D3 and D11 are fixed. D4 and the error signalling of D7 and D9 are the best candidates for the next intents,
followed by D8 and D12. Each would be handled as its own intent with an ADR and an update of this file and
of `docs/spec/`.
