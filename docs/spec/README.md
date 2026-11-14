# Spec index

One file per functional area describing delivered behaviour, with the tests that prove it.

- [transport-and-sessions.md](transport-and-sessions.md): startup and shutdown, connect and disconnect, reconnect, keepalive,
  client identity, broker loss, unknown senders, malformed datagrams.
- [topics-and-pubsub.md](topics-and-pubsub.md): topic registration, QoS 0 and QoS 1 publish in both directions,
  subscriptions with wildcards and short names, gateway registration for downlink topics.
- [will.md](will.md): will handshake and will publication.
- [sleep.md](sleep.md): sleep entry and expiry, downlink buffering, wake-up flush.
- [discovery.md](discovery.md): SEARCHGW/GWINFO and periodic ADVERTISE.
- [configuration.md](configuration.md): YAML configuration keys, defaults and validation.
- [operations.md](operations.md): version flag, release binaries, container image, CI and releases.
