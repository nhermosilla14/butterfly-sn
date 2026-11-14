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
		defer srv.Close()
		fn(srv)
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
