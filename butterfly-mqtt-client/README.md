# butterfly-mqtt-client

Minimal MQTT 3.1.1 client for Go: CONNECT/CONNACK (with optional will), DISCONNECT, QoS 0 and QoS 1
PUBLISH/PUBACK, PINGREQ/PINGRESP, and QoS 0 SUBSCRIBE/SUBACK with incoming PUBLISH reads over TCP.
Packet reads use full MQTT framing. Standard library only. It knows nothing about MQTT-SN or gateways.

```go
c, err := mqtt.Dial(ctx, "127.0.0.1:1883", 5*time.Second)
if err != nil { ... }
err = c.Connect(ctx, mqtt.ConnectOptions{ClientID: "device-1", CleanSession: true, KeepAlive: 60})
if err != nil { ... } // *mqtt.ConnackError when the broker refuses
defer c.Close()

err = c.Publish("sensors/temperature", []byte("21.5"), false)
```

Set `WillTopic`, `WillPayload`, `WillQoS` (0 or 1) and `WillRetain` in `ConnectOptions` to register a
will. `PublishQoS1` and `ReadEvent` carry the QoS 1 uplink; `Pingreq` and `ReadEvent` carry keepalive.
`Subscribe` and `ReadPublish` provide the QoS 0 observer path. Read operations must not run concurrently
with each other. Test with `go test ./...` and fuzz the packet reader with `go test -fuzz=FuzzReadPacket`.
