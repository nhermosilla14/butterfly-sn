# Reject will updates

- Intents: 20261114-will-update-rejection

## Scope
WILLTOPICUPD (including the empty form) and WILLMSGUPD from a session get WILLTOPICRESP and WILLMSGRESP with
Not supported. Nothing else changes: the broker connection, the stored will and the session state are untouched.
Asleep clients ignore them, unknown senders get DISCONNECT, malformed ones are ignored.

## Layers touched
- Foundation: `internal/mqttsn` decodes and encodes the four messages.
- Services: `internal/gateway` session loop answers them; log without will contents.
- Interface: MQTT-SN datagrams.
- Docs: `docs/architecture.md`; `docs/vision.md` moves will updates to non-goals.

## End-to-end test
- Codec round-trip tests for the four messages, including the two-octet empty WILLTOPICUPD.
- Gateway tests with a real broker connection and a will-bearing client: WILLTOPICUPD, empty WILLTOPICUPD and
  WILLMSGUPD each get Not supported; the session keeps working (publish reaches the broker, PINGREQ answered);
  the broker connection is not reopened; losing the client afterwards still publishes the original will. Asleep
  client gets no answer; unknown sender gets DISCONNECT; malformed update is ignored.
- Covers all acceptance criteria.

## Out of scope
- Applying updates.
