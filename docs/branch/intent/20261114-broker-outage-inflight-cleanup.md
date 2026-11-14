# Broker outage: retransmission and in-flight cleanup

- Status: Accepted
- Date: 2026-11-14
- Owner: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Depends on: none

## Goal
A broker or a client that stops answering never leaves a session stuck or its in-flight state leaking. Every
exchange the gateway starts (REGISTER and PUBLISH towards the client, SUBSCRIBE and UNSUBSCRIBE towards the
broker) has a bounded wait: it is retried where the protocol allows it, and when the peer stays silent the
exchange is cleaned up and, if the peer is the client, the session ends. Broker outages already end the session
(see transport-and-sessions); this intent closes the gaps in the window before the outage is detected and for
state that survives it.

## Acceptance criteria
- An unanswered gateway REGISTER is retransmitted every retry interval, up to the retry limit. After the last
  attempt the client is declared lost: it gets DISCONNECT and the session is torn down like any other client
  loss, which discards the held messages and the topic name with it. Nothing is acknowledged to the broker
  during that teardown.
- An unanswered downlink PUBLISH QoS 1 is retransmitted with the same msg id and the DUP flag, on the same
  schedule. When the retries run out the client is declared lost as above and the broker PUBACK is not sent.
- A REGISTER or PUBLISH awaiting a reply during a wake-up flush follows the same rule, so a flush can no longer
  wait for a missing REGACK or PUBACK forever; PINGRESP is still the last datagram when the flush completes.
- A SUBSCRIBE or UNSUBSCRIBE forwarded to the broker and not answered within the full retry budget (retry
  interval times retry limit) means the broker is not responding: the gateway sends the client DISCONNECT and
  tears the session down like any other broker loss. It is not retransmitted, because MQTT over TCP does not
  retransmit.
- Pending entries and held messages are freed when the session ends for any reason, including broker loss, so a
  new CONNECT with the same client id starts with no leftover state.
- `retry_interval` (seconds, default 10) and `retry_limit` (default 5, the number of retransmissions) are new
  configuration keys, within the specification's best-practice ranges as defaults. Non-positive values fail
  startup with a message naming the key.
- Timers do not fire for a sleeping client except during a flush, and a client that answers in time sees no
  duplicate.
- Retries, give-ups and cleanup are logged with the client id and message type, never payload bytes.

## Spec changes
- Adds: topics-and-pubsub / Downlink retransmission
- Changes: topics-and-pubsub / Subscribe and unsubscribe (pending timeout ends the session)
- Changes: sleep / Wake-up flush (bounded waits)
- Changes: transport-and-sessions / Broker connection loss (unanswered broker request is a loss; state released on teardown)
- Changes: configuration / Minimal YAML configuration (retry keys)
- Removes: topics-and-pubsub, sleep / Not supported entries on retransmission

## Out of scope
- Reconnecting to the broker on behalf of the client or resuming sessions; the client sends CONNECT again.
- Fast failing CONNECT while the broker is down, and backoff across clients.
- Gateway-originated PINGREQ towards active clients.
- Retransmission of uplink PUBLISH to the broker: the client owns that retry.
- Persisting buffered or in-flight messages across gateway restarts.

## Decisions taken
Checked against `olddocs/MQTT-SN_spec_v1.2.pdf`:
- 6.13 defines Tretry and Nretry for the client only; 7.2 gives the recommended values (Tretry 10-15 s, Nretry 3-5).
  The specification has no gateway-side retransmission rule. 6.10 says only that the gateway waits for the REGACK
  before sending the PUBLISH, with no timeout and no outcome when none arrives. 6.9 leaves SUBSCRIBE and
  UNSUBSCRIBE retransmission to the client, and nothing covers a broker that stops answering.
- What it does allow: the DUP flag marks a retransmitted PUBLISH (5.3.4), and the lost state with will activation
  comes from the keep-alive and sleep timers (6.14). A gateway DISCONNECT is a recognised unsolicited message, and
  the client is expected to send CONNECT again (6.12).
- Where the specification is silent, the rule is that the session ends. So gateway retransmission uses the
  client procedure's Tretry, Nretry and 7.2 values, and exhausting it ends the session for every message type,
  including QoS 0 REGISTER, with the teardown used for any client loss (DISCONNECT to the client, will published).
  An unanswered SUBSCRIBE or UNSUBSCRIBE also ends the session.
