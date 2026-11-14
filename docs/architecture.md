# Architecture

Current state: no code yet. The architecture is decided slice by slice through ADRs; this file will
describe what exists.

Intended shape (to be confirmed by ADRs):

```
 MQTT-SN clients --UDP--> [ gateway: session manager ] --TCP, one connection per client--> MQTT broker
```

Reference for the legacy design: `olddocs/technical/architecture.md`.
