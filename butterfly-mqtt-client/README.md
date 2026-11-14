# butterfly-mqtt-client

Minimal MQTT 3.1.1 client for Go: CONNECT, CONNACK and DISCONNECT over TCP, with framed packet reads.
Standard library only. It knows nothing about MQTT-SN or gateways.

```go
c, err := mqtt.Dial(ctx, "127.0.0.1:1883", 5*time.Second)
if err != nil { ... }
err = c.Connect(ctx, mqtt.ConnectOptions{ClientID: "device-1", CleanSession: true, KeepAlive: 60})
if err != nil { ... } // *mqtt.ConnackError when the broker refuses
defer c.Close()
```

Test with `go test ./...` and fuzz the packet reader with `go test -fuzz=FuzzReadPacket`.
