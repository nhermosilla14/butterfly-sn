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
	reg := &pendingReg{topic: e.Topic, topicID: id, held: []mqtt.Event{e}, fromSleep: true, seq: s.regSeq}
	s.pendingRegs[msgID] = reg
	s.regByTopic[e.Topic] = msgID
	if err := s.sendSupervised(kindRegister, msgID, mqttsn.Register{TopicID: id, MsgID: msgID, TopicName: e.Topic}); err != nil {
		log.Warn("dropped buffered PUBLISH: REGISTER too large to send", "msg_id", msgID, "error", err)
		s.forgetRegister(msgID, reg)
	}
	return true
}

// sendFlushed sends a buffered PUBLISH to the client on topicID of type topicIDType. The broker was acknowledged
// when the message was buffered, so a QoS 1 message gets a fresh gateway msg id: the broker may have reused its
// packet id since. The msg id goes in preAcked, so the client's PUBACK produces no second broker PUBACK. When
// preAcked is full the QoS 1 message is dropped and logged; it is not sent and is not acknowledged again. A message
// too large to send is dropped the same way.
func (s *session) sendFlushed(topicIDType byte, topicID uint16, e mqtt.Event, log *slog.Logger) {
	if e.QoS == 0 {
		m := mqttsn.Publish{Retain: e.Retain, TopicIDType: topicIDType, TopicID: topicID, Data: e.Payload}
		if err := s.gw.send(s.addr, m); err != nil {
			log.Warn("dropped buffered PUBLISH: too large to send", "error", err)
		}
		return
	}
	if len(s.preAcked) >= maxPreAcked {
		log.Warn("pre-acknowledged set full, dropping buffered QoS 1 PUBLISH")
		return
	}
	msgID := s.nextFlushMsgID()
	m := mqttsn.Publish{QoS: 1, Retain: e.Retain, DUP: e.DUP, TopicIDType: topicIDType, TopicID: topicID, MsgID: msgID, Data: e.Payload}
	if err := s.sendSupervised(kindPublish, msgID, m); err != nil {
		log.Warn("dropped buffered PUBLISH: too large to send", "msg_id", msgID, "error", err)
		return
	}
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

// interruptFlush ends a running flush for a re-sleep. The messages held for pending sleep-origin REGISTERs go back to
// the front of the buffer in registration order. They were acknowledged to the broker when first buffered, so they
// are not acknowledged again. Their topic names are forgotten and the retry set is cleared; QoS 1 PUBLISHes awaiting
// the client's PUBACK are parked as on any sleep entry, and pre-acknowledged ids stay, so a late PUBACK is absorbed.
// Every registration of a flush is sleep-origin, since broker PUBLISHes are buffered while flushing. The buffer cap
// still applies: messages beyond it, at the back, are dropped and logged.
func (s *session) interruptFlush(log *slog.Logger) {
	var front []mqtt.Event
	for _, reg := range s.sortedRegs() {
		front = append(front, reg.held...)
		if id, ok := s.topics.idOf(reg.topic); ok && id == reg.topicID {
			s.topics.remove(reg.topic)
		}
	}
	s.pendingRegs = make(map[uint16]*pendingReg)
	s.regByTopic = make(map[string]uint16)
	s.clearRetries()
	s.sleepBuf = append(front, s.sleepBuf...)
	if n := len(s.sleepBuf) - maxSleepBuffer; n > 0 {
		log.Warn("sleep buffer full, dropping newest PUBLISHes", "dropped", n)
		s.sleepBuf = s.sleepBuf[:maxSleepBuffer]
	}
}

// popBuffer removes the head of the buffer.
func (s *session) popBuffer() {
	s.sleepBuf[0] = mqtt.Event{}
	s.sleepBuf = s.sleepBuf[1:]
}
