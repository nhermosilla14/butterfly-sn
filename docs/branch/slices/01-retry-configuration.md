# Retry configuration

- Intents: 20261114-broker-outage-inflight-cleanup
- ADRs: 20261114-gateway-retry-and-pending-timeouts

## Scope
The YAML file accepts `retry_interval` (seconds, default 10) and `retry_limit` (default 5). Non-positive values
fail startup naming the key. The gateway keeps the interval as a duration field that tests can shorten.

## Layers touched
- Foundation: `internal/config` keys, defaults and validation; `config.example.yaml` and README key table.
- Services: `internal/gateway` reads the values into fields; nothing uses them yet.
- Interface: the configuration file.

## End-to-end test
- Config tests for defaults, explicit values and invalid values (zero, negative), plus a startup test through
  `internal/app` that a bad `retry_limit` exits non-zero with the key named.
- Covers criteria: configurable retry keys, invalid values fail startup.

## Out of scope
- Any retry behaviour.
