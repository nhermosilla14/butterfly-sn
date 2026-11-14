package e2e

import (
	"testing"

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
