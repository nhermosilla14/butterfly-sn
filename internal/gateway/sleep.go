package gateway

import (
	"log/slog"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// maxSleepBuffer bounds the broker PUBLISHes buffered per session while the client sleeps.
const maxSleepBuffer = 20

// bufferDownlink holds a broker PUBLISH for a sleeping client. QoS 1 is acknowledged to the broker at once, so
// the broker's in-flight window does not stall while the client sleeps. When the buffer is full the newest
// message is dropped and logged; a QoS 1 one was already acknowledged, so that loss is accepted. Nothing is sent
// to the client. Payloads and topic names are never logged. An error means the broker write failed.
func (s *session) bufferDownlink(broker *mqtt.Client, e mqtt.Event, log *slog.Logger) error {
	if len(s.sleepBuf) >= maxSleepBuffer {
		log.Warn("sleep buffer full, dropping newest PUBLISH", "qos", e.QoS)
	} else {
		s.sleepBuf = append(s.sleepBuf, e)
		if e.QoS == 1 {
			s.preAcked[e.PacketID] = struct{}{}
		}
	}
	if e.QoS != 1 {
		return nil
	}
	return s.pubackBroker(broker, e.PacketID)
}

// ignoredWhileAsleep reports whether a client packet must be ignored while the client sleeps. DISCONNECT is
// always handled by the session loop, so it is not ignored. PINGREQ and CONNECT are the wake-up packets of a
// later slice: they are logged and change nothing for now.
func ignoredWhileAsleep(m mqttsn.Message, log *slog.Logger) bool {
	switch m.(type) {
	case mqttsn.Disconnect:
		return false
	case mqttsn.Pingreq, mqttsn.Connect:
		log.Info("packet from sleeping client ignored: wake-up not implemented yet", "type", m)
	default:
		log.Debug("ignored packet from sleeping client", "type", m)
	}
	return true
}
