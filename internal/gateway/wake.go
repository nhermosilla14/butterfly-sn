package gateway

import (
	"log/slog"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// maxPreAcked bounds the gateway msg ids of buffered QoS 1 PUBLISHes awaiting the client's PUBACK.
const maxPreAcked = 20

// advanceFlush delivers buffered PUBLISHes in order, from the head of the buffer. It stops at the first message
// that must wait for a REGACK, and it waits while a sleep-origin REGISTER is outstanding. It returns true when the
// buffer is empty and no sleep-origin REGISTER remains, so the flush is complete.
func (s *session) advanceFlush(log *slog.Logger) bool {
	for len(s.sleepBuf) > 0 && !s.sleepRegPending() {
		if !s.flushHead(log) {
			break
		}
	}
	return len(s.sleepBuf) == 0 && !s.sleepRegPending()
}

// flushHead delivers the head of the buffer and removes it. It returns false, leaving the head in place, when the
// message must wait for a REGACK already outstanding for its topic. Otherwise it returns true: the message was sent,
// or dropped because the table is full, or a sleep-origin REGISTER now holds it.
func (s *session) flushHead(log *slog.Logger) bool {
	e := s.sleepBuf[0]
	if id, ok := shortID(e.Topic); ok {
		if _, short := s.shortSubs[e.Topic]; short {
			s.popBuffer()
			s.sendFlushed(mqttsn.TopicIDShort, id, e, log)
			return true
		}
	}
	if _, held := s.regByTopic[e.Topic]; held {
		return false
	}
	if id, ok := s.topics.idOf(e.Topic); ok {
		s.popBuffer()
		s.sendFlushed(mqttsn.TopicIDNormal, id, e, log)
		return true
	}
	s.popBuffer()
	id, ok := s.topics.register(e.Topic)
	if !ok {
		log.Warn("topic table full, dropping buffered PUBLISH")
		return true
	}
	msgID := s.nextRegMsgID()
	s.regSeq++
	s.pendingRegs[msgID] = &pendingReg{topic: e.Topic, topicID: id, held: []mqtt.Event{e}, fromSleep: true, seq: s.regSeq}
	s.regByTopic[e.Topic] = msgID
	s.sendSupervised(kindRegister, msgID, mqttsn.Register{TopicID: id, MsgID: msgID, TopicName: e.Topic})
	return true
}

// sendFlushed sends a buffered PUBLISH to the client on topicID of type topicIDType. The broker was acknowledged
// when the message was buffered, so a QoS 1 message gets a fresh gateway msg id: the broker may have reused its
// packet id since. The msg id goes in preAcked, so the client's PUBACK produces no second broker PUBACK. When
// preAcked is full the QoS 1 message is dropped and logged; it is not sent and is not acknowledged again.
func (s *session) sendFlushed(topicIDType byte, topicID uint16, e mqtt.Event, log *slog.Logger) {
	if e.QoS == 0 {
		s.gw.sendTo(s.addr, mqttsn.Publish{Retain: e.Retain, TopicIDType: topicIDType, TopicID: topicID, Data: e.Payload})
		return
	}
	if len(s.preAcked) >= maxPreAcked {
		log.Warn("pre-acknowledged set full, dropping buffered QoS 1 PUBLISH")
		return
	}
	msgID := s.nextFlushMsgID()
	s.sendSupervised(kindPublish, msgID, mqttsn.Publish{QoS: 1, Retain: e.Retain, DUP: e.DUP, TopicIDType: topicIDType, TopicID: topicID, MsgID: msgID, Data: e.Payload})
	s.preAcked[msgID] = struct{}{}
}

// nextFlushMsgID returns the next gateway msg id for a buffered QoS 1 PUBLISH: 1..65535, never 0. An id still
// awaiting the client's PUBACK, in pendingDownlink or preAcked, is skipped. Both sets hold at most 20 ids, so a
// free id is always found.
func (s *session) nextFlushMsgID() uint16 {
	for {
		s.flushMsgID++
		if s.flushMsgID == 0 {
			s.flushMsgID = 1
		}
		_, busyDown := s.pendingDownlink[s.flushMsgID]
		_, busyAcked := s.preAcked[s.flushMsgID]
		if !busyDown && !busyAcked {
			return s.flushMsgID
		}
	}
}

// preAckedBusy reports whether a gateway msg id of a buffered PUBLISH still awaits the client's PUBACK.
func (s *session) preAckedBusy(id uint16) bool {
	_, busy := s.preAcked[id]
	return busy
}

// sleepRegPending reports whether a sleep-origin REGISTER still awaits its REGACK.
func (s *session) sleepRegPending() bool {
	for _, reg := range s.pendingRegs {
		if reg.fromSleep {
			return true
		}
	}
	return false
}

// popBuffer removes the head of the buffer.
func (s *session) popBuffer() {
	s.sleepBuf[0] = mqtt.Event{}
	s.sleepBuf = s.sleepBuf[1:]
}
