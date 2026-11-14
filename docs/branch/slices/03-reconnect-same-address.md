# Reconnect from the same address

- Status: Done
- Intents: 20261114-conformance-defect-fixes
- ADRs: 20261114-reconnect-from-same-address

## Scope
A CONNECT that reaches an active session after its CONNACK replaces it: clean MQTT DISCONNECT to the broker, no
client DISCONNECT, atomic swap in the gateway, new will handshake, broker connection, duration and CONNACK. Earlier
CONNECTs (retransmissions) and CONNECTs during the will handshake are ignored; sleeping clients are unchanged.

## Layers touched
- Services: the Connect case in the session loop, a gateway replace method under its mutex, CONNACK time tracking.
- Interface: MQTT-SN datagrams.
- Docs: `docs/architecture.md`.

## End-to-end test
- Gateway tests with a real broker: a second CONNECT from the same socket gets CONNACK and a new broker
  connection while the old one is closed without publishing its will (observer subscribed to the will topic); the
  new duration and will apply; works with keep-alive 0; at `max_clients` 1 the replacement is accepted; the old
  client gets no DISCONNECT; a CONNECT queued before CONNACK is ignored; a sleeping client's CONNECT still wakes.
- Covers the D3 criteria.

## Out of scope
- Everything else in CONFORMANCE.md.
