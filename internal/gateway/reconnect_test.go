package gateway

import (
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// TestReconnectConnectBeforeConnackIgnored: a CONNECT that arrives while the broker is still being dialled is a
// retransmission of the CONNECT that started the session. It is ignored, so the client gets exactly one CONNACK and
// the broker is not asked again.
func TestReconnectConnectBeforeConnackIgnored(t *testing.T) {
	r := startWillRig(t, 10)
	connect := mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "reconnect-early"}
	sendSN(t, r.client, r.gwAddr, connect)
	r.broker.nextConnect(t) // the broker has the CONNECT and holds its CONNACK
	sendSN(t, r.client, r.gwAddr, connect)
	time.Sleep(200 * time.Millisecond) // let the retransmission reach the session inbox

	r.broker.allow()
	readAs[mqttsn.Connack](t, r.client)
	expectSilence(t, r.client)
}
