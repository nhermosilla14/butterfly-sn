# Re-sleep with a new duration

- Status: Done
- Intents: 20261114-conformance-defect-fixes
- ADRs: 20261114-sleep-state-buffer-flush-and-resleep

## Scope
DISCONNECT with a duration above 0 from a sleeping or awake client changes the sleep duration and puts it back to
sleep, keeping session, broker connection, topic table and buffer. A running flush is interrupted without losing
or duplicating messages and without a second broker PUBACK. DISCONNECT without a duration still ends the session.

## Layers touched
- Services: the Disconnect case in the session loop and `holdToSleep`/flush state in `internal/gateway`.
- Interface: MQTT-SN datagrams.
- Docs: `docs/architecture.md`.

## End-to-end test
- Gateway tests with a real broker: sleep, re-sleep with a shorter duration, expiry follows the new duration and
  publishes the will; re-sleep while asleep keeps buffered messages that arrive on the next PINGREQ; re-sleep in
  the middle of a flush with an unanswered REGISTER keeps the messages in order with no second broker PUBACK, and
  the next wake delivers them; a late PUBACK of an already sent QoS 1 flush message is absorbed; DISCONNECT with
  duration 0 still ends the session. The existing test that encoded the old behaviour is replaced.
- Covers the D1 criteria.

## Out of scope
- CONNECT from a sleeping client (D4).
