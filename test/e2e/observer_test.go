package e2e

import (
	"context"
	"testing"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
)

// observer is a plain MQTT subscriber connected straight to the broker. It checks what the broker received.
type observer struct {
	t  *testing.T
	cl *mqtt.Client
}

// newObserver connects clientID to the broker and subscribes to filter at QoS 0.
func newObserver(t *testing.T, brokerAddr, clientID, filter string) *observer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()
	cl, err := mqtt.Dial(ctx, brokerAddr, waitShort)
	if err != nil {
		t.Fatalf("observer dial: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	if err := cl.Connect(ctx, mqtt.ConnectOptions{ClientID: clientID, CleanSession: true, KeepAlive: 60}); err != nil {
		t.Fatalf("observer connect: %v", err)
	}
	if err := cl.Subscribe(ctx, filter); err != nil {
		t.Fatalf("observer subscribe %q: %v", filter, err)
	}
	return &observer{t: t, cl: cl}
}

// next waits up to waitLong for the next delivery from the broker.
func (o *observer) next() mqtt.Message {
	o.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitLong)
	defer cancel()
	m, err := o.cl.ReadPublish(ctx)
	if err != nil {
		o.t.Fatalf("waiting for broker delivery: %v", err)
	}
	return m
}
