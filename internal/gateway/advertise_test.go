package gateway

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/config"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

const testTick = 100 * time.Millisecond

// startAdvertiser runs a gateway (id 7) advertising every testTick to a fresh UDP listener, whose
// configured interval is secs. It returns the listener, a stop function and Run's result channel.
func startAdvertiser(t *testing.T, secs int) (*net.UDPConn, context.CancelFunc, <-chan error) {
	t.Helper()
	saved := advertiseTick
	advertiseTick = func(int) time.Duration { return testTick }
	t.Cleanup(func() { advertiseTick = saved })

	dest, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dest.Close() })
	cfg := config.Config{Listen: "127.0.0.1:0", Broker: "127.0.0.1:1", MaxClients: 10, GatewayID: 7,
		AdvertiseInterval: secs, AdvertiseAddress: dest.LocalAddr().String()}
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	stopped := make(chan struct{})
	go func() { done <- g.Run(ctx); close(stopped) }()
	t.Cleanup(func() {
		cancel()
		<-stopped // Run must end before advertiseTick is restored
	})
	return dest, cancel, done
}

func TestAdvertiseArrivesPeriodicallyAfterOneInterval(t *testing.T) {
	start := time.Now()
	dest, _, _ := startAdvertiser(t, 900)
	var last time.Time
	for i := range 3 {
		m, err := recvSN(t, dest, testWaitLong)
		if err != nil {
			t.Fatalf("ADVERTISE %d: %v", i, err)
		}
		now := time.Now()
		if got, ok := m.(mqttsn.Advertise); !ok || got.GwID != 7 || got.Duration != 900 {
			t.Fatalf("got %+v, want ADVERTISE id 7 duration 900", m)
		}
		if i == 0 && now.Sub(start) < testTick-10*time.Millisecond {
			t.Fatalf("first ADVERTISE after %v, want at least one interval", now.Sub(start))
		}
		if i > 0 && now.Sub(last) < testTick/2 {
			t.Fatalf("ADVERTISE gap %v, want about %v", now.Sub(last), testTick)
		}
		last = now
	}
}

func TestAdvertiseDisabledSendsNothing(t *testing.T) {
	dest, _, _ := startAdvertiser(t, 0)
	if m, err := recvSN(t, dest, 5*testTick); err == nil {
		t.Fatalf("got %+v with interval 0", m)
	}
}

func TestAdvertiseDefaultAddressUsesBoundPort(t *testing.T) {
	g, err := New(config.Config{Listen: "127.0.0.1:0", Broker: "127.0.0.1:1", MaxClients: 1, GatewayID: 1, AdvertiseInterval: 1},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run with broadcast default: %v", err)
	}
}

func TestAdvertiseShutdownStopsTicker(t *testing.T) {
	for range 20 {
		dest, cancel, done := startAdvertiser(t, 1)
		time.Sleep(testTick + 20*time.Millisecond)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
		for {
			if _, err := recvSN(t, dest, time.Millisecond); err != nil {
				break
			}
		}
		if m, err := recvSN(t, dest, testTick); err == nil {
			t.Fatalf("datagram %+v after Run returned", m)
		}
	}
}
