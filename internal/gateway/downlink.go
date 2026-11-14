package gateway

import (
	"fmt"
	"log/slog"
	"time"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// maxHeldDownlink bounds the broker PUBLISHes held across all topics awaiting REGACK, per session.
// maxPendingDownlink bounds the QoS 1 PUBLISHes sent to the client that await its PUBACK, per session.
const (
	maxHeldDownlink    = 20
	maxPendingDownlink = 20
)

// pendingReg is a gateway REGISTER awaiting the client's REGACK, with the broker PUBLISHes held for it in
// arrival order.
type pendingReg struct {
	topic   string
	topicID uint16
	held    []mqtt.Event
}

// nextRegMsgID returns the next gateway REGISTER msg id: 1..65535, never 0. An id still awaiting REGACK is
// skipped, so a wrap cannot overwrite it.
func (s *session) nextRegMsgID() uint16 {
	for {
		s.regMsgID++
		if s.regMsgID == 0 {
			s.regMsgID = 1
		}
		if _, busy := s.pendingRegs[s.regMsgID]; !busy {
			return s.regMsgID
		}
	}
}

// heldCount returns the number of broker PUBLISHes held across all pending registrations.
func (s *session) heldCount() int {
	n := 0
	for _, reg := range s.pendingRegs {
		n += len(reg.held)
	}
	return n
}

// handleDownlink delivers one broker PUBLISH to the client. A topic awaiting REGACK joins its hold. A topic in
// the table is sent at once with its id. An unknown topic gets a gateway REGISTER and is held until REGACK.
// Messages that cannot be delivered are dropped and logged; QoS 1 ones are acknowledged to the broker.
// Payloads and topic names are never logged. An error means a write to the broker failed.
func (s *session) handleDownlink(broker *mqtt.Client, e mqtt.Event, log *slog.Logger) error {
	// A duplicate id is dropped without a broker ack: acknowledging it would acknowledge the in-flight message
	// that still awaits the client's PUBACK under the same id, before the client has answered it.
	if e.QoS == 1 && s.downlinkAwaitingPuback(e.PacketID) {
		log.Warn("dropped downlink PUBLISH: packet id still awaiting client PUBACK", "packet_id", e.PacketID)
		return nil
	}
	if id, ok := shortID(e.Topic); ok {
		if _, short := s.shortSubs[e.Topic]; short {
			return s.sendDownlinkTo(broker, mqttsn.TopicIDShort, id, e, log)
		}
	}
	if msgID, held := s.regByTopic[e.Topic]; held {
		if s.heldCount() >= maxHeldDownlink {
			log.Warn("downlink hold full, dropping PUBLISH")
			return s.dropDownlink(broker, e)
		}
		reg := s.pendingRegs[msgID]
		reg.held = append(reg.held, e)
		return nil
	}
	if id, ok := s.topics.idOf(e.Topic); ok {
		return s.sendDownlink(broker, id, e, log)
	}
	if s.heldCount() >= maxHeldDownlink {
		log.Warn("downlink hold full, dropping PUBLISH")
		return s.dropDownlink(broker, e)
	}
	id, ok := s.topics.register(e.Topic)
	if !ok {
		log.Warn("topic table full, dropping PUBLISH")
		return s.dropDownlink(broker, e)
	}
	msgID := s.nextRegMsgID()
	s.pendingRegs[msgID] = &pendingReg{topic: e.Topic, topicID: id, held: []mqtt.Event{e}}
	s.regByTopic[e.Topic] = msgID
	s.gw.sendTo(s.addr, mqttsn.Register{TopicID: id, MsgID: msgID, TopicName: e.Topic})
	return nil
}

// sendDownlink sends a broker PUBLISH to the client on the normal topic id topicID.
func (s *session) sendDownlink(broker *mqtt.Client, topicID uint16, e mqtt.Event, log *slog.Logger) error {
	return s.sendDownlinkTo(broker, mqttsn.TopicIDNormal, topicID, e, log)
}

// sendDownlinkTo sends a broker PUBLISH to the client on topicID of type topicIDType. QoS 1 uses the broker packet
// id as msg id and is recorded in pendingDownlink until the client's PUBACK. A QoS 1 message is dropped, and the
// broker acknowledged, when pendingDownlink is full.
func (s *session) sendDownlinkTo(broker *mqtt.Client, topicIDType byte, topicID uint16, e mqtt.Event, log *slog.Logger) error {
	if e.QoS == 0 {
		s.gw.sendTo(s.addr, mqttsn.Publish{Retain: e.Retain, TopicIDType: topicIDType, TopicID: topicID, Data: e.Payload})
		return nil
	}
	if s.downlinkAwaitingPuback(e.PacketID) {
		log.Warn("dropped downlink PUBLISH: packet id still awaiting client PUBACK", "packet_id", e.PacketID)
		return nil
	}
	if len(s.pendingDownlink) >= maxPendingDownlink {
		log.Warn("too many downlink PUBLISH awaiting client PUBACK, dropping")
		return s.dropDownlink(broker, e)
	}
	s.gw.sendTo(s.addr, mqttsn.Publish{QoS: 1, Retain: e.Retain, DUP: e.DUP, TopicIDType: topicIDType, TopicID: topicID, MsgID: e.PacketID, Data: e.Payload})
	s.pendingDownlink[e.PacketID] = topicID
	return nil
}

// downlinkAwaitingPuback reports whether a QoS 1 downlink with this broker packet id awaits the client's PUBACK.
func (s *session) downlinkAwaitingPuback(id uint16) bool {
	_, busy := s.pendingDownlink[id]
	return busy
}

// dropDownlink discards a broker PUBLISH that cannot be delivered. A QoS 1 message is acknowledged to the
// broker so it does not retry forever. QoS 0 needs no acknowledgement.
func (s *session) dropDownlink(broker *mqtt.Client, e mqtt.Event) error {
	if e.QoS != 1 {
		return nil
	}
	return s.pubackBroker(broker, e.PacketID)
}

// pubackBroker sends MQTT PUBACK for a QoS 1 PUBLISH received from the broker.
func (s *session) pubackBroker(broker *mqtt.Client, id uint16) error {
	if err := broker.Puback(id); err != nil {
		return fmt.Errorf("PUBACK: %w", err)
	}
	s.lastWrite = time.Now()
	return nil
}

// handleRegack answers the gateway REGISTER with msg id m.MsgID. Accepted releases the held PUBLISHes for that
// topic in arrival order. Rejected drops them, acknowledging QoS 1 ones to the broker, and removes the name from
// the table. An unknown msg id is logged and ignored.
func (s *session) handleRegack(broker *mqtt.Client, m mqttsn.Regack, log *slog.Logger) error {
	reg, ok := s.pendingRegs[m.MsgID]
	if !ok {
		log.Warn("ignored REGACK for unknown msg id", "msg_id", m.MsgID)
		return nil
	}
	delete(s.pendingRegs, m.MsgID)
	delete(s.regByTopic, reg.topic)
	if m.ReturnCode != mqttsn.ReturnAccepted {
		log.Warn("client rejected topic registration, dropping held PUBLISHes", "return_code", m.ReturnCode, "topic_id", reg.topicID)
		for _, e := range reg.held {
			if err := s.dropDownlink(broker, e); err != nil {
				return err
			}
		}
		// The client does not know this id, so forget the name: a later PUBLISH registers it afresh.
		if id, ok := s.topics.idOf(reg.topic); ok && id == reg.topicID {
			s.topics.remove(reg.topic)
		}
		return nil
	}
	for _, e := range reg.held {
		if err := s.sendDownlink(broker, reg.topicID, e, log); err != nil {
			return err
		}
	}
	return nil
}

// handleClientPuback acknowledges a downlink QoS 1 PUBLISH to the broker once the client has PUBACKed it.
// A rejected return code is logged, but the broker is still acknowledged so it does not retry forever. An
// unknown msg id is logged and ignored.
func (s *session) handleClientPuback(broker *mqtt.Client, m mqttsn.Puback, log *slog.Logger) error {
	if !s.downlinkAwaitingPuback(m.MsgID) {
		log.Warn("ignored client PUBACK for unknown msg id", "msg_id", m.MsgID)
		return nil
	}
	delete(s.pendingDownlink, m.MsgID)
	if m.ReturnCode != mqttsn.ReturnAccepted {
		log.Warn("client rejected downlink PUBLISH", "return_code", m.ReturnCode)
	}
	return s.pubackBroker(broker, m.MsgID)
}
