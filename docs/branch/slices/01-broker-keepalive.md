# Keep the broker connection alive

- Status: In progress
- Intents: 20261114-session-liveness
- ADRs: 20261114-session-liveness-and-broker-loss, 20261114-broker-event-reader-api
- Depends on: none

## Scope
An idle MQTT-SN client stays connected: the gateway keeps its dedicated broker connection alive with MQTT
PINGREQ of its own, so the broker does not drop the client for silence. A client whose CONNECT duration is 0
gets no gateway keepalive traffic and no keepalive is requested from the broker. Existing QoS 0/1 publishing
keeps working while the reader handles several broker packet types in order.

## Layers touched
- Services: `butterfly-mqtt-client` gains MQTT PINGREQ/PINGRESP, `Pingreq()` and one ordered `ReadEvent` that
  returns PUBACK, PINGRESP or DISCONNECT with strict validation; `ReadPuback` is replaced. The session reader
  loop forwards these events to the session goroutine. The session schedules a broker PINGREQ when half the
  client keepalive has passed without a broker write, tracks the expected PINGRESP, and stops pinging when the
  keepalive is 0.
- Interface: none; the UDP wire behaviour is unchanged.

## End-to-end test
- An idle client connects with a 2 s keepalive, waits several keepalive periods without sending anything, then
  publishes at QoS 1; the broker receives it and the client gets PUBACK. Without gateway pings the broker would
  have dropped the connection.
- Covers criteria: idle client stays usable; keepalive 0 disables gateway broker pings (also unit-tested).

## Out of scope
- Reacting to broker loss: a lost broker still leaves the session in place; the next slice fixes it.
- Client inactivity timeout, MQTT-SN PINGREQ/PINGRESP and best-effort DISCONNECT on timeout.
- Sleep, will, retransmission.
