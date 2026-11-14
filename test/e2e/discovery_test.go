package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/app"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// TestSearchgwGetsGwinfo: SEARCHGW, with and without radius, gets exactly one GWINFO with the configured gateway
// id and creates no session, so a later REGISTER from the same socket gets DISCONNECT.
func TestSearchgwGetsGwinfo(t *testing.T) {
	b := startBroker(t)
	gw := startGatewayExtra(t, b.addr, 10, "gateway_id: 42\n")
	cases := map[string]mqttsn.Searchgw{
		"no radius":   {},
		"with radius": {Radius: 2, HasRadius: true},
	}
	for name, sg := range cases {
		t.Run(name, func(t *testing.T) {
			c := newSNClient(t, gw.addr)
			c.send(sg)
			got := expect[mqttsn.Gwinfo](t, c, waitLong)
			if got.GwID != 42 {
				t.Fatalf("gateway id %d, want 42", got.GwID)
			}
			expectNoDatagram(t, c, downlinkWindow)

			c.send(mqttsn.Register{MsgID: 1, TopicName: "e2e/discovery/searchgw/" + name})
			expect[mqttsn.Disconnect](t, c, waitLong)
		})
	}
}

// TestGwinfoAndAdvertiseFromClientIgnored: client-sent GWINFO and ADVERTISE get no reply and create no session.
func TestGwinfoAndAdvertiseFromClientIgnored(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)

	c.send(mqttsn.Gwinfo{GwID: 9})
	c.send(mqttsn.Advertise{GwID: 9, Duration: 900})
	expectNoDatagram(t, c, downlinkWindow)

	c.send(mqttsn.Register{MsgID: 1, TopicName: "e2e/discovery/ignored"})
	expect[mqttsn.Disconnect](t, c, waitLong)
}

// advertiseSink is a UDP socket on 127.0.0.1 that receives the gateway's ADVERTISE.
func advertiseSink(t *testing.T) (*snClient, string) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &snClient{t: t, conn: conn}, conn.LocalAddr().String()
}

// TestAdvertisePeriodic: with interval 1 ADVERTISE carries the gateway id and Duration 1, comes after a full
// interval, and repeats about every second.
func TestAdvertisePeriodic(t *testing.T) {
	b := startBroker(t)
	sink, addr := advertiseSink(t)
	start := time.Now()
	startGatewayExtra(t, b.addr, 10, fmt.Sprintf("gateway_id: 42\nadvertise_interval: 1\nadvertise_address: %s\n", addr))

	got := expect[mqttsn.Advertise](t, sink, waitShort)
	first := time.Now()
	if got.GwID != 42 || got.Duration != 1 {
		t.Fatalf("got %+v, want gateway id 42 duration 1", got)
	}
	if first.Sub(start) < 900*time.Millisecond {
		t.Fatalf("first ADVERTISE after %v, want about one interval", first.Sub(start))
	}
	expect[mqttsn.Advertise](t, sink, waitShort)
	if gap := time.Since(first); gap < 800*time.Millisecond || gap > 1500*time.Millisecond {
		t.Fatalf("second ADVERTISE %v after the first, want about 1s", gap)
	}
}

// TestAdvertiseDisabled: interval 0 sends nothing.
func TestAdvertiseDisabled(t *testing.T) {
	b := startBroker(t)
	sink, addr := advertiseSink(t)
	startGatewayExtra(t, b.addr, 10, fmt.Sprintf("advertise_interval: 0\nadvertise_address: %s\n", addr))
	expectNoDatagram(t, sink, 2500*time.Millisecond)
}

// TestStartupInvalidAdvertise: bad advertise settings fail startup naming the key.
func TestStartupInvalidAdvertise(t *testing.T) {
	cases := map[string]struct{ extra, key string }{
		"interval too big": {"advertise_interval: 65536\n", "advertise_interval"},
		"interval neg":     {"advertise_interval: -1\n", "advertise_interval"},
		"address no port":  {"advertise_address: 10.0.0.1\n", "advertise_address"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			cfg := writeConfig(t, "listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\n"+tc.extra)
			if code := app.Run(context.Background(), []string{"-config", cfg}, &out); code == 0 {
				t.Fatalf("exit status 0, want non-zero; output: %s", out.String())
			}
			if !strings.Contains(out.String(), tc.key) {
				t.Fatalf("output does not mention %q: %s", tc.key, out.String())
			}
		})
	}
}
