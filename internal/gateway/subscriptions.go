package gateway

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// maxPendingSubscriptions and maxPendingUnsubscriptions bound the requests per session that await the
// broker's SUBACK or UNSUBACK.
const (
	maxPendingSubscriptions   = 20
	maxPendingUnsubscriptions = 20
)

// isWildcard reports whether a topic filter contains a wildcard character.
func isWildcard(filter string) bool {
	return strings.ContainsAny(filter, "+#")
}

// handleSubscribe forwards a SUBSCRIBE to the broker on this session's connection. QoS 2 and predefined or short
// topic ids are not supported yet: they get SUBACK Not supported without broker contact. An exact topic name is
// registered in the session's table; a wildcard filter is not, and its SUBACK reports topic id 0. A request with
// msg id 0 is dropped, and so is one whose msg id is still pending. An error means the broker write failed.
func (s *session) handleSubscribe(broker *mqtt.Client, m mqttsn.Subscribe, log *slog.Logger) error {
	if m.MsgID == 0 {
		log.Debug("dropped SUBSCRIBE with msg id 0")
		return nil
	}
	if m.QoS > 1 || m.TopicIDType != mqttsn.TopicIDNormal {
		log.Debug("refused SUBSCRIBE: only QoS 0 and 1 with normal topic id are supported", "qos", m.QoS, "topic_id_type", m.TopicIDType)
		s.gw.sendTo(s.addr, mqttsn.Suback{MsgID: m.MsgID, ReturnCode: mqttsn.ReturnNotSupported})
		return nil
	}
	if _, busy := s.pendingSubs[m.MsgID]; busy {
		log.Warn("dropped SUBSCRIBE: msg id still awaiting broker SUBACK", "msg_id", m.MsgID)
		return nil
	}
	if len(s.pendingSubs) >= maxPendingSubscriptions {
		log.Warn("too many pending SUBSCRIBE requests, rejecting")
		s.gw.sendTo(s.addr, mqttsn.Suback{MsgID: m.MsgID, ReturnCode: mqttsn.ReturnCongestion})
		return nil
	}
	var topicID uint16
	if !isWildcard(m.TopicName) {
		id, ok := s.topics.register(m.TopicName)
		if !ok {
			log.Warn("topic table full, rejecting SUBSCRIBE")
			s.gw.sendTo(s.addr, mqttsn.Suback{MsgID: m.MsgID, ReturnCode: mqttsn.ReturnCongestion})
			return nil
		}
		topicID = id
	}
	if err := broker.SubscribeRequest(m.TopicName, m.QoS, m.MsgID); err != nil {
		return fmt.Errorf("SUBSCRIBE: %w", err)
	}
	s.lastWrite = time.Now()
	s.pendingSubs[m.MsgID] = topicID
	return nil
}

// handleSuback answers the client's SUBSCRIBE with the broker's result. A refusal (0x80) becomes SUBACK with
// return code 0x02, QoS 0 and topic id 0. An unknown packet id is logged and ignored.
func (s *session) handleSuback(e mqtt.Event, log *slog.Logger) {
	id := e.PacketID
	topicID, ok := s.pendingSubs[id]
	if !ok {
		log.Warn("ignored broker SUBACK for unknown packet id", "packet_id", id)
		return
	}
	delete(s.pendingSubs, id)
	if e.ReturnCode == mqtt.SubackError {
		log.Warn("broker refused subscription")
		s.gw.sendTo(s.addr, mqttsn.Suback{MsgID: id, ReturnCode: mqttsn.ReturnInvalidTopicID})
		return
	}
	s.gw.sendTo(s.addr, mqttsn.Suback{QoS: e.GrantedQoS, TopicID: topicID, MsgID: id, ReturnCode: mqttsn.ReturnAccepted})
}

// handleUnsubscribe forwards an UNSUBSCRIBE of a normal topic name or filter to the broker. The client's topic
// table entry is kept, because its topic id stays valid for publishing. Predefined and short topic ids are not
// supported: UNSUBACK is sent at once without broker contact, since UNSUBACK has no return code. A request
// whose msg id is still pending is dropped silently, since UNSUBACK cannot report a refusal. An error means the
// broker write failed.
func (s *session) handleUnsubscribe(broker *mqtt.Client, m mqttsn.Unsubscribe, log *slog.Logger) error {
	if m.MsgID == 0 {
		log.Debug("dropped UNSUBSCRIBE with msg id 0")
		return nil
	}
	if m.TopicIDType != mqttsn.TopicIDNormal {
		log.Debug("UNSUBSCRIBE with unsupported topic id type acknowledged without broker contact", "topic_id_type", m.TopicIDType)
		s.gw.sendTo(s.addr, mqttsn.Unsuback{MsgID: m.MsgID})
		return nil
	}
	if _, busy := s.pendingUnsubs[m.MsgID]; busy {
		log.Warn("dropped UNSUBSCRIBE: msg id still awaiting broker UNSUBACK", "msg_id", m.MsgID)
		return nil
	}
	if len(s.pendingUnsubs) >= maxPendingUnsubscriptions {
		log.Warn("too many pending UNSUBSCRIBE requests, dropping")
		return nil
	}
	if err := broker.UnsubscribeRequest(m.TopicName, m.MsgID); err != nil {
		return fmt.Errorf("UNSUBSCRIBE: %w", err)
	}
	s.lastWrite = time.Now()
	s.pendingUnsubs[m.MsgID] = struct{}{}
	return nil
}

// handleUnsuback answers the client's UNSUBSCRIBE with the same msg id. An unknown packet id is logged and ignored.
func (s *session) handleUnsuback(e mqtt.Event, log *slog.Logger) {
	id := e.PacketID
	if _, ok := s.pendingUnsubs[id]; !ok {
		log.Warn("ignored broker UNSUBACK for unknown packet id", "packet_id", id)
		return
	}
	delete(s.pendingUnsubs, id)
	s.gw.sendTo(s.addr, mqttsn.Unsuback{MsgID: id})
}
