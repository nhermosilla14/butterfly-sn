# SEARCHGW and unicast GWINFO

- Status: In progress
- Intents: 20261114-discovery
- ADRs: 20261114-gateway-discovery
- Depends on: none

## Scope
A client that searches for a gateway gets a unicast GWINFO after a short random delay, and discovery packets stop
being mistaken for client traffic. The gateway id comes from configuration.

## Layers touched
- Services: `internal/mqttsn` SEARCHGW and GWINFO codec; router discovery handling before session lookup;
  configuration key `gateway_id`; delayed unicast send.
- Interface: none beyond the existing UDP socket.

## End-to-end test
- SEARCHGW, with and without a radius, gets one GWINFO with the configured gateway id and no session is created;
  a following packet from the same sender still follows the unknown-sender rule.
- GWINFO and ADVERTISE from an unknown sender get no reply and create no session.
- Covers criteria: SEARCHGW/GWINFO, radius ignored, random delay bound, gateway id, ignored discovery packets.

## Out of scope
- Periodic ADVERTISE and its configuration; next slice.
- Multicast, suppression of other gateways, rate limiting.
