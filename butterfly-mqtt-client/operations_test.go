package mqtt

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestPublishEncodesQoS0(t *testing.T) {
	got := make(chan Packet, 1)
	c := serve(t, func(srv net.Conn) {
		p, err := ReadPacket(srv)
		if err != nil {
			t.Error(err)
			return
		}
		got <- p
	})
	payload := []byte{0, 0xFF, '\r', '\n', 0x80}
	if err := c.Publish("a/b", payload, true); err != nil {
		t.Fatal(err)
	}
	p := <-got
	want := append([]byte{0, 3, 'a', '/', 'b'}, payload...)
	if p.Type != typePublish || p.Flags != flagRetain || !bytes.Equal(p.Body, want) {
		t.Fatalf("PUBLISH: type %d flags %#x body % x", p.Type, p.Flags, p.Body)
	}
}

func TestPublishEmptyPayloadNoRetain(t *testing.T) {
	got := make(chan Packet, 1)
	c := serve(t, func(srv net.Conn) {
		p, _ := ReadPacket(srv)
		got <- p
	})
	if err := c.Publish("t", nil, false); err != nil {
		t.Fatal(err)
	}
	if p := <-got; p.Flags != 0 || !bytes.Equal(p.Body, []byte{0, 1, 't'}) {
		t.Fatalf("PUBLISH: flags %#x body % x", p.Flags, p.Body)
	}
}

func TestPublishRejectsInvalidTopic(t *testing.T) {
	c := &Client{}
	for _, topic := range []string{"", "a/+", "a/#", "#"} {
		if err := c.Publish(topic, nil, false); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("Publish(%q): got %v, want ErrInvalidTopic", topic, err)
		}
	}
}

func allBytes() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func TestSubscribeAndReadPublish(t *testing.T) {
	sub := make(chan Packet, 1)
	c := serve(t, func(srv net.Conn) {
		p, err := ReadPacket(srv)
		if err != nil {
			t.Error(err)
			return
		}
		sub <- p
		suback, _ := packet(typeSuback, 0, []byte{0, 1, 0})
		_, _ = srv.Write(suback)
		pub, _ := packet(typePublish, flagRetain, append([]byte{0, 3, 'x', '/', 'y'}, allBytes()...))
		_, _ = srv.Write(pub)
		pub, _ = packet(typePublish, 0, []byte{0, 1, 'z'})
		_, _ = srv.Write(pub)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Subscribe(ctx, "x/y"); err != nil {
		t.Fatal(err)
	}
	p := <-sub
	want := []byte{0, 1, 0, 3, 'x', '/', 'y', 0}
	if p.Type != typeSubscribe || p.Flags != flagSubscribe || !bytes.Equal(p.Body, want) {
		t.Fatalf("SUBSCRIBE: type %d flags %#x body % x", p.Type, p.Flags, p.Body)
	}
	m, err := c.ReadPublish(ctx)
	if err != nil || m.Topic != "x/y" || !m.Retain || !bytes.Equal(m.Payload, allBytes()) {
		t.Fatalf("ReadPublish: %+v, %v", m, err)
	}
	m, err = c.ReadPublish(ctx)
	if err != nil || m.Topic != "z" || m.Retain || len(m.Payload) != 0 {
		t.Fatalf("second ReadPublish: %+v, %v", m, err)
	}
}

func TestSubscribeRefused(t *testing.T) {
	c := serve(t, func(srv net.Conn) {
		_, _ = ReadPacket(srv)
		suback, _ := packet(typeSuback, 0, []byte{0, 1, 0x80})
		_, _ = srv.Write(suback)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Subscribe(ctx, "x"); !errors.Is(err, ErrSubscribeRefused) {
		t.Fatalf("got %v, want ErrSubscribeRefused", err)
	}
}

func TestSubscribeWrongPacketID(t *testing.T) {
	c := serve(t, func(srv net.Conn) {
		_, _ = ReadPacket(srv)
		suback, _ := packet(typeSuback, 0, []byte{0, 9, 0})
		_, _ = srv.Write(suback)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Subscribe(ctx, "x"); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("got %v, want ErrMalformedPacket", err)
	}
}

func TestReadPublishRejectsQoS1AndMalformed(t *testing.T) {
	cases := map[string]Packet{
		"qos 1":          {Type: typePublish, Flags: 0x02, Body: []byte{0, 1, 'a', 0, 1}},
		"not publish":    {Type: typeDisconnect},
		"short body":     {Type: typePublish, Body: []byte{0}},
		"topic too long": {Type: typePublish, Body: []byte{0, 9, 'a'}},
	}
	for name, p := range cases {
		c := serve(t, func(srv net.Conn) {
			b, _ := packet(p.Type, p.Flags, p.Body)
			_, _ = srv.Write(b)
		})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.ReadPublish(ctx)
		cancel()
		if err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestReadPublishTimesOut(t *testing.T) {
	c := serve(t, func(srv net.Conn) {
		<-time.After(5 * time.Second)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := c.ReadPublish(ctx); err == nil {
		t.Fatal("no error on silent broker")
	}
}

func TestPublishQoS1EncodesPacket(t *testing.T) {
	got := make(chan Packet, 1)
	c := serve(t, func(srv net.Conn) {
		p, err := ReadPacket(srv)
		if err != nil {
			t.Error(err)
			return
		}
		got <- p
	})
	payload := []byte{0, 0xFF, '\r', '\n', 0x80}
	if err := c.PublishQoS1("a/b", payload, true, true, 0xBEEF); err != nil {
		t.Fatal(err)
	}
	p := <-got
	want := append([]byte{0, 3, 'a', '/', 'b', 0xBE, 0xEF}, payload...)
	if p.Type != typePublish || p.Flags != 0x08|0x02|0x01 || !bytes.Equal(p.Body, want) {
		t.Fatalf("PUBLISH: type %d flags %#x body % x", p.Type, p.Flags, p.Body)
	}
}

func TestPublishQoS1PlainFlags(t *testing.T) {
	got := make(chan Packet, 1)
	c := serve(t, func(srv net.Conn) {
		p, _ := ReadPacket(srv)
		got <- p
	})
	if err := c.PublishQoS1("t", nil, false, false, 1); err != nil {
		t.Fatal(err)
	}
	if p := <-got; p.Flags != 0x02 || !bytes.Equal(p.Body, []byte{0, 1, 't', 0, 1}) {
		t.Fatalf("PUBLISH: flags %#x body % x", p.Flags, p.Body)
	}
}

func TestPublishQoS1RejectsInvalid(t *testing.T) {
	c := &Client{}
	if err := c.PublishQoS1("a/#", nil, false, false, 1); !errors.Is(err, ErrInvalidTopic) {
		t.Errorf("wildcard topic: got %v, want ErrInvalidTopic", err)
	}
	if err := c.PublishQoS1("a", nil, false, false, 0); !errors.Is(err, ErrMalformedPacket) {
		t.Errorf("packet id 0: got %v, want ErrMalformedPacket", err)
	}
}

// TestPublishQoS1WriteError checks that a broken connection is reported, not swallowed.
func TestPublishQoS1WriteError(t *testing.T) {
	c := serve(t, func(srv net.Conn) {})
	_ = c.conn.Close()
	if err := c.PublishQoS1("a", []byte("x"), false, false, 1); err == nil {
		t.Fatal("no error writing to closed connection")
	}
}

func TestPingreqEncodesPacket(t *testing.T) {
	got := make(chan Packet, 1)
	c := serve(t, func(srv net.Conn) {
		p, err := ReadPacket(srv)
		if err != nil {
			t.Error(err)
			return
		}
		got <- p
	})
	if err := c.Pingreq(); err != nil {
		t.Fatal(err)
	}
	if p := <-got; p.Type != typePingreq || p.Flags != 0 || len(p.Body) != 0 {
		t.Fatalf("PINGREQ: type %d flags %#x body % x", p.Type, p.Flags, p.Body)
	}
}

func TestReadEventReturnsBrokerPacketsInArrivalOrder(t *testing.T) {
	published := make(chan struct{})
	c := serve(t, func(srv net.Conn) {
		_, _ = ReadPacket(srv)
		close(published)
		ack, _ := packet(typePuback, 0, []byte{0x12, 0x34})
		pingresp, _ := packet(typePingresp, 0, nil)
		disconnect, _ := packet(typeDisconnect, 0, nil)
		_, _ = srv.Write(append(append(ack, pingresp...), disconnect...))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.PublishQoS1("a", nil, false, false, 0x1234); err != nil {
		t.Fatal(err)
	}
	<-published
	want := []Event{
		{Kind: EventPuback, PacketID: 0x1234},
		{Kind: EventPingresp},
		{Kind: EventDisconnect},
	}
	for _, w := range want {
		got, err := c.ReadEvent(ctx)
		if err != nil || got != w {
			t.Fatalf("ReadEvent: %+v, %v; want %+v", got, err, w)
		}
	}
}

func TestReadEventMalformed(t *testing.T) {
	cases := map[string][]byte{
		"publish type":     {0x30, 0x02, 0, 1},
		"puback flags":     {0x41, 0x02, 0, 1},
		"puback short":     {0x40, 0x01, 0},
		"puback long":      {0x40, 0x03, 0, 1, 0},
		"pingresp flags":   {0xD1, 0x00},
		"pingresp body":    {0xD0, 0x01, 0},
		"disconnect flags": {0xE1, 0x00},
		"disconnect body":  {0xE0, 0x01, 0},
		"connack":          {0x20, 0x02, 0, 0},
	}
	for name, raw := range cases {
		c := serve(t, func(srv net.Conn) {
			_, _ = srv.Write(raw)
		})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.ReadEvent(ctx)
		cancel()
		if !errors.Is(err, ErrMalformedPacket) {
			t.Errorf("%s: got %v, want ErrMalformedPacket", name, err)
		}
	}
}

func TestReadEventCancelled(t *testing.T) {
	c := serve(t, func(srv net.Conn) {})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.ReadEvent(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}
