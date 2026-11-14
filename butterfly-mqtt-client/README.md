# butterfly-mqtt-client

Minimal MQTT 3.1.1 client for Go: CONNECT/CONNACK, DISCONNECT, QoS 0 PUBLISH, and QoS 0
SUBSCRIBE/SUBACK with incoming PUBLISH reads over TCP. Packet reads use full MQTT framing.
Standard library only. It knows nothing about MQTT-SN or gateways.

```go
c, err := mqtt.Dial(ctx, "127.0.0.1:1883", 5*time.Second)
if err != nil { ... }
err = c.Connect(ctx, mqtt.ConnectOptions{ClientID: "device-1", CleanSession: true, KeepAlive: 60})
if err != nil { ... } // *mqtt.ConnackError when the broker refuses
defer c.Close()

err = c.Publish("sensors/temperature", []byte("21.5"), false)
```

`Subscribe` and `ReadPublish` provide the QoS 0 observer path. They must not run concurrently with
each other. Test with `go test ./...` and fuzz the packet reader with `go test -fuzz=FuzzReadPacket`.
