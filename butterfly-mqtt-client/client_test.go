package mqtt

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// serve runs fn on the broker end of a pipe and returns the client end.
func serve(t *testing.T, fn func(net.Conn)) *Client {
	t.Helper()
	cli, srv := net.Pipe()
	go func() {
		// Keep the broker end open until the client closes: net.Pipe deadlines fail once the peer
		// is gone, which would make Connect's final deadline reset race with the server exiting.
		defer srv.Close()
		fn(srv)
		_, _ = io.Copy(io.Discard, srv)
	}()
	c := &Client{conn: cli}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestConnectEncodesCONNECT(t *testing.T) {
	got := make(chan Packet, 1)
	c := serve(t, func(srv net.Conn) {
		p, err := ReadPacket(srv)
		if err != nil {
			t.Error(err)
			return
		}
		got <- p
		ack, _ := packet(typeConnack, 0, []byte{0, 0})
		_, _ = srv.Write(ack)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Connect(ctx, ConnectOptions{ClientID: "abc", CleanSession: true, KeepAlive: 60}); err != nil {
		t.Fatal(err)
	}
	p := <-got
	want := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0x02, 0, 60, 0, 3, 'a', 'b', 'c'}
	if p.Type != typeConnect || !bytes.Equal(p.Body, want) {
		t.Fatalf("CONNECT: type %d body %v", p.Type, p.Body)
	}
}

// connectPacket runs Connect against a broker that accepts it and returns the CONNECT packet it received.
func connectPacket(t *testing.T, opts ConnectOptions) Packet {
	t.Helper()
	got := make(chan Packet, 1)
	c := serve(t, func(srv net.Conn) {
		p, err := ReadPacket(srv)
		if err != nil {
			t.Error(err)
			return
		}
		got <- p
		ack, _ := packet(typeConnack, 0, []byte{0, 0})
		_, _ = srv.Write(ack)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Connect(ctx, opts); err != nil {
		t.Fatal(err)
	}
	return <-got
}

func TestConnectEncodesWill(t *testing.T) {
	p := connectPacket(t, ConnectOptions{ClientID: "abc", CleanSession: true, KeepAlive: 60,
		WillTopic: "w/t", WillPayload: []byte("bye"), WillQoS: 1, WillRetain: true})
	// Flags: clean session 0x02, will 0x04, will QoS 1 at bits 3-4 (0x08), will retain 0x20.
	want := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0x02 | 0x04 | 0x08 | 0x20, 0, 60,
		0, 3, 'a', 'b', 'c', 0, 3, 'w', '/', 't', 0, 3, 'b', 'y', 'e'}
	if p.Type != typeConnect || !bytes.Equal(p.Body, want) {
		t.Fatalf("CONNECT with will: type %d body % x, want % x", p.Type, p.Body, want)
	}
}

func TestConnectWillFlags(t *testing.T) {
	cases := []struct {
		name  string
		opts  ConnectOptions
		flags byte
	}{
		{"no will", ConnectOptions{ClientID: "x"}, 0x00},
		{"empty topic no will", ConnectOptions{ClientID: "x", CleanSession: true}, 0x02},
		{"qos 0", ConnectOptions{ClientID: "x", WillTopic: "t"}, 0x04},
		{"qos 1", ConnectOptions{ClientID: "x", WillTopic: "t", WillQoS: 1}, 0x04 | 0x08},
		{"qos 0 retain", ConnectOptions{ClientID: "x", WillTopic: "t", WillRetain: true}, 0x04 | 0x20},
		{"qos 1 retain", ConnectOptions{ClientID: "x", WillTopic: "t", WillQoS: 1, WillRetain: true}, 0x04 | 0x08 | 0x20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := connectPacket(t, tc.opts)
			if p.Body[7] != tc.flags {
				t.Fatalf("connect flags %#x, want %#x", p.Body[7], tc.flags)
			}
		})
	}
}

func TestConnectWillInvalid(t *testing.T) {
	cases := map[string]struct {
		opts ConnectOptions
		want error
	}{
		"qos 2":                   {ConnectOptions{ClientID: "x", WillTopic: "t", WillQoS: 2}, ErrInvalidWill},
		"qos 3":                   {ConnectOptions{ClientID: "x", WillTopic: "t", WillQoS: 3}, ErrInvalidWill},
		"qos without topic":       {ConnectOptions{ClientID: "x", WillQoS: 1}, ErrInvalidWill},
		"retain without topic":    {ConnectOptions{ClientID: "x", WillRetain: true}, ErrInvalidWill},
		"payload without topic":   {ConnectOptions{ClientID: "x", WillPayload: []byte("p")}, ErrInvalidWill},
		"wildcard in will topic":  {ConnectOptions{ClientID: "x", WillTopic: "a/+"}, ErrInvalidTopic},
		"will payload over 65535": {ConnectOptions{ClientID: "x", WillTopic: "t", WillPayload: make([]byte, 0x10000)}, ErrMalformedPacket},
	}
	for name, tc := range cases {
		// No connection: an invalid CONNECT must fail before anything is written.
		err := (&Client{}).Connect(context.Background(), tc.opts)
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: got %v, want %v", name, err, tc.want)
		}
	}
}

func TestConnectReturnCodeRefused(t *testing.T) {
	c := serve(t, func(srv net.Conn) {
		_, _ = ReadPacket(srv)
		ack, _ := packet(typeConnack, 0, []byte{0, 5})
		_, _ = srv.Write(ack)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Connect(ctx, ConnectOptions{ClientID: "x"})
	var ce *ConnackError
	if !errors.As(err, &ce) || ce.Code != 5 {
		t.Fatalf("got %v, want ConnackError code 5", err)
	}
}

func TestConnectMalformedReply(t *testing.T) {
	c := serve(t, func(srv net.Conn) {
		_, _ = ReadPacket(srv)
		pkt, _ := packet(typeDisconnect, 0, nil)
		_, _ = srv.Write(pkt)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Connect(ctx, ConnectOptions{ClientID: "x"}); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("got %v, want ErrMalformedPacket", err)
	}
}

func TestConnectTimesOut(t *testing.T) {
	c := serve(t, func(srv net.Conn) {
		_, _ = ReadPacket(srv)
		<-time.After(5 * time.Second)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.Connect(ctx, ConnectOptions{ClientID: "x"}); err == nil {
		t.Fatal("no error on silent broker")
	}
}

func TestDisconnectSendsDISCONNECTAndCloses(t *testing.T) {
	got := make(chan Packet, 1)
	cli, srv := net.Pipe()
	go func() {
		p, err := ReadPacket(srv)
		if err != nil {
			t.Error(err)
		}
		got <- p
		_, _ = io.Copy(io.Discard, srv) // returns when the client closes
	}()
	c := &Client{conn: cli}
	if err := c.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if p := <-got; p.Type != typeDisconnect || len(p.Body) != 0 {
		t.Fatalf("got %+v, want empty DISCONNECT", p)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestDialTimeoutAndRefused(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	if _, err := Dial(context.Background(), addr, time.Second); err == nil {
		t.Fatal("dial to closed port succeeded")
	}
}
