# Broker request timeouts and teardown cleanup

- Intents: 20261114-broker-outage-inflight-cleanup
- ADRs: 20261114-gateway-retry-and-pending-timeouts
- Depends on: 01

## Scope
A SUBSCRIBE or UNSUBSCRIBE the broker does not answer within `retry_interval` times `retry_limit` ends the
session with DISCONNECT to the client, as for any broker loss. A late answer is ignored. After any session end a
new CONNECT with the same client id starts with no pending state.

## Layers touched
- Foundation: send times on pending subscriptions and unsubscriptions, folded into the session timer of slice 02.
- Services: deadline check in the session loop; logging.
- Interface: MQTT-SN datagrams, and a test broker that stops answering.

## End-to-end test
- Gateway test with a stub broker that accepts the connection and never answers SUBSCRIBE: the client gets
  DISCONNECT after the budget and the slot is free. Same for UNSUBSCRIBE. An answer inside the budget leaves the
  session alive. A reconnect with the same client id after a loss with pending REGISTER, SUBSCRIBE and downlink
  state starts clean (a first publish needs a new REGISTER and an old REGACK is ignored).
- Covers criteria: broker request timeout ends the session, state released on teardown.

## Out of scope
- Retransmitting to the broker, reconnecting for the client, fast-failing CONNECT during an outage.
