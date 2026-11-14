# Gateway discovery: SEARCHGW, GWINFO and ADVERTISE

- Status: In progress
- Date: 2026-11-14
- Owner: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Depends on: none

## Goal
A client can discover the gateway by sending SEARCHGW and receives a unicast GWINFO after a short random delay.
The gateway also advertises itself periodically on its own interval, configured separately from any client
keepalive, so clients that listen for ADVERTISE can find it without broadcasting a search.

## Acceptance criteria
- SEARCHGW from any sender, with or without a radius byte, is answered with one GWINFO unicast to that sender
  after a random delay of at most 500 ms. The radius value is ignored.
- The gateway id in GWINFO and ADVERTISE is configurable from 1 to 255 and defaults to 1.
- GWINFO and ADVERTISE received from clients or other gateways are logged and ignored: they create no session and
  get no DISCONNECT.
- A periodic ADVERTISE carries the gateway id and a duration equal to the configured interval in seconds.
  `advertise_interval` defaults to 900 s; 0 disables periodic advertisement. Intervals above 65535 s are refused.
- The first ADVERTISE is sent after one full interval, not at startup, and the ticker stops on shutdown.
- The advertisement destination is configurable as a UDP `host:port`. By default it is the IPv4 broadcast address
  on the gateway's listen port, so clients on the local segment can hear it; deployments that route discovery
  differently can point it at another address.
- Discovery never creates, changes or removes a client session and never contacts the broker.
- Configuration errors (gateway id outside 1–255, negative or oversized interval, invalid advertisement address)
  make startup fail with a message naming the key.

## Spec changes
- Adds: discovery / SEARCHGW and GWINFO
- Adds: discovery / ADVERTISE
- Changes: configuration / Minimal YAML configuration
- Changes: transport-and-sessions / Unknown or disconnected senders
- Changes: transport-and-sessions / Not supported
- Changes: configuration / Not supported

## Out of scope
- Multicast group membership; discovery uses unicast replies and a configurable advertisement destination.
- Suppressing a pending GWINFO when another gateway announces itself first.
- Gateway radius semantics beyond ignoring the field, gateway name, and discovery authentication.
- Retransmission or rate limiting for SEARCHGW floods.
- Will updates, retransmission, delivery and releases.

## Decisions taken
- GWINFO is unicast to the searching sender, replacing the legacy multicast reply.
- The random delay is uniform in 0–500 ms, a fixed constant with no configuration key.
- ADVERTISE has its own interval and does not reuse any client keepalive, fixing the legacy coupling.
- The advertisement destination defaults to the IPv4 broadcast address on the listen port; `advertise_address`
  exists for routed networks and tests.
- Cross-gateway suppression is deferred; the legacy gateway had none either.
