package gateway

import (
	"log/slog"
	"sort"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// maxSleepBuffer bounds the broker PUBLISHes buffered per session while the client sleeps or is flushed.
const maxSleepBuffer = 20

// bufferDownlink holds a broker PUBLISH for a sleeping client, or one arriving during a wake flush. QoS 1 is
// acknowledged to the broker at once, so the broker's in-flight window does not stall while the client sleeps.
// When the buffer is full the newest message is dropped and logged; a QoS 1 one was already acknowledged, so that
// loss is accepted. Nothing is sent to the client here. Payloads and topic names are never logged. An error means
// the broker write failed.
func (s *session) bufferDownlink(broker *mqtt.Client, e mqtt.Event, log *slog.Logger) error {
	if len(s.sleepBuf) >= maxSleepBuffer {
		log.Warn("sleep buffer full, dropping newest PUBLISH", "qos", e.QoS)
	} else {
		s.sleepBuf = append(s.sleepBuf, e)
	}
	if e.QoS != 1 {
		return nil
	}
	return s.pubackBroker(broker, e.PacketID)
}

// holdToSleep moves the PUBLISHes held for pending gateway REGISTERs into the sleep buffer, in the order the
// registrations were made, and clears the registrations. A registration the client never answered is forgotten,
// so the next wake registers its topic afresh. A late REGACK for it is then an unknown msg id. Each held message
// goes through bufferDownlink, so QoS 1 is acknowledged here and a full buffer drops it. An error means the broker
// write failed.
func (s *session) holdToSleep(broker *mqtt.Client, log *slog.Logger) error {
	regs := make([]*pendingReg, 0, len(s.pendingRegs))
	for _, reg := range s.pendingRegs {
		regs = append(regs, reg)
	}
	sort.Slice(regs, func(i, j int) bool { return regs[i].seq < regs[j].seq })
	for _, reg := range regs {
		for _, e := range reg.held {
			if err := s.bufferDownlink(broker, e, log); err != nil {
				return err
			}
		}
		if id, ok := s.topics.idOf(reg.topic); ok && id == reg.topicID {
			s.topics.remove(reg.topic)
		}
	}
	s.pendingRegs = make(map[uint16]*pendingReg)
	s.regByTopic = make(map[string]uint16)
	s.clearRetries()
	return nil
}

// ignoredWhileAsleep reports whether a client packet must be ignored while the client sleeps. DISCONNECT is always
// handled. PINGREQ and CONNECT wake the client. During a PINGREQ flush REGACK and PUBACK are handled, as is a PUBACK
// for a PUBLISH sent on wake, which may arrive after the flush. Everything else is logged and dropped.
func (s *session) ignoredWhileAsleep(m mqttsn.Message, log *slog.Logger) bool {
	switch p := m.(type) {
	case mqttsn.Disconnect:
		return false
	case mqttsn.Regack:
		if s.flushing {
			return false
		}
	case mqttsn.Puback:
		// A PUBACK for a PUBLISH sent on wake may arrive after the flush has ended.
		if _, ok := s.preAcked[p.MsgID]; ok || s.flushing {
			return false
		}
	case mqttsn.Pingreq, mqttsn.Connect:
		if !s.flushing {
			return false
		}
	}
	log.Debug("ignored packet from sleeping client", "type", m)
	return true
}
