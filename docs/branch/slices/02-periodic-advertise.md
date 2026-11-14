# Periodic ADVERTISE

- Intents: 20261114-discovery
- ADRs: 20261114-gateway-discovery
- Depends on: 01

## Scope
The gateway advertises itself periodically with its own interval, independent of client keepalive, to a
configurable UDP destination. Advertisement is off when the interval is 0, starts after one full interval and
stops on shutdown.

## Layers touched
- Services: `internal/mqttsn` ADVERTISE codec; configuration keys `advertise_interval` (0–65535, default 900) and
  `advertise_address` (default the broadcast address on the listen port); one gateway-level ticker owned by `Run`.
- Interface: config file and the UDP socket.

## End-to-end test
- With a short configured interval and a test UDP destination, ADVERTISE arrives with the gateway id and the
  interval as its 16-bit duration, and keeps arriving on the interval.
- Interval 0 sends nothing within a bounded window.
- Bad gateway id, interval and address fail startup with the key named.
- Covers criteria: periodic advertisement, duration, starts after one interval, shutdown, destination,
  configuration validation.

## Out of scope
- Multicast group membership, cross-gateway suppression, discovery rate limiting.
