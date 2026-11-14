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
	// maxShortSubscriptions bounds the short names tracked per session, counting pending short SUBSCRIBEs.
	maxShortSubscriptions = 50
)

// isWildcard reports whether a topic filter contains a wildcard character.
func isWildcard(filter string) bool {
	return strings.ContainsAny(filter, "+#")
}

// isPublishableName reports whether a topic name may be sent to the broker in a PUBLISH or REGISTER: it is not
// empty and has no wildcard character. The broker client refuses such names, so they never reach it.
func isPublishableName(name string) bool {
	return name != "" && !isWildcard(name)
}

// trackedShortNames returns the short names this session is subscribed to or has requested and awaits SUBACK
// for. It is the set the maxShortSubscriptions cap counts.
func (s *session) trackedShortNames() map[string]struct{} {
	names := make(map[string]struct{}, len(s.shortSubs))
	for name := range s.shortSubs {
		names[name] = struct{}{}
	}
	for _, pend := range s.pendingSubs {
		if pend.short != "" {
			names[pend.short] = struct{}{}
		}
	}
	return names
}

// pendingSub is a SUBSCRIBE awaiting the broker's SUBACK: the topic id to report and, for a short subscription,
// its name, which is tracked only once the broker accepts it.
type pendingSub struct {
	topicID uint16
	short   string
}

// handleSubscribe forwards a SUBSCRIBE to the broker on this session's connection. QoS 2 and predefined topic ids
// are not supported yet: they get SUBACK Not supported without broker contact. An exact topic name is registered in
// the session's table; a wildcard filter is not, and its SUBACK reports topic id 0. A short name must be two
// characters (see shortID) and is tracked in shortSubs instead of the table; any other length gets SUBACK
// Congestion without broker contact. A request with msg id 0 is dropped, and so is one whose msg id is still
// pending. An error means the broker write failed.
func (s *session) handleSubscribe(broker *mqtt.Client, m mqttsn.Subscribe, log *slog.Logger) error {
	if m.MsgID == 0 {
		log.Debug("dropped SUBSCRIBE with msg id 0")
		return nil
	}
	if m.QoS > 1 || (m.TopicIDType != mqttsn.TopicIDNormal && m.TopicIDType != mqttsn.TopicIDShort) {
		log.Debug("refused SUBSCRIBE: only QoS 0 and 1 with normal or short topic id are supported", "qos", m.QoS, "topic_id_type", m.TopicIDType)
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
	var pend pendingSub
	switch {
	case m.TopicIDType == mqttsn.TopicIDShort:
		id, ok := shortID(m.TopicName)
		if !ok {
			log.Warn("refused SUBSCRIBE: invalid short topic name")
			s.gw.sendTo(s.addr, mqttsn.Suback{MsgID: m.MsgID, ReturnCode: mqttsn.ReturnCongestion})
			return nil
		}
		tracked := s.trackedShortNames()
		if _, known := tracked[m.TopicName]; !known && len(tracked) >= maxShortSubscriptions {
			log.Warn("too many short subscriptions, rejecting SUBSCRIBE")
			s.gw.sendTo(s.addr, mqttsn.Suback{MsgID: m.MsgID, ReturnCode: mqttsn.ReturnCongestion})
			return nil
		}
		pend = pendingSub{topicID: id, short: m.TopicName}
	case !isWildcard(m.TopicName):
		id, ok := s.topics.register(m.TopicName)
		if !ok {
			log.Warn("topic table full, rejecting SUBSCRIBE")
			s.gw.sendTo(s.addr, mqttsn.Suback{MsgID: m.MsgID, ReturnCode: mqttsn.ReturnCongestion})
			return nil
		}
		pend.topicID = id
	}
	if err := broker.SubscribeRequest(m.TopicName, m.QoS, m.MsgID); err != nil {
		return fmt.Errorf("SUBSCRIBE: %w", err)
	}
	s.lastWrite = time.Now()
	s.pendingSubs[m.MsgID] = pend
	return nil
}

// handleSuback answers the client's SUBSCRIBE with the broker's result. A refusal (0x80) becomes SUBACK with
// return code 0x02, QoS 0 and topic id 0. An accepted short subscription is tracked in shortSubs. An unknown
// packet id is logged and ignored.
func (s *session) handleSuback(e mqtt.Event, log *slog.Logger) {
	id := e.PacketID
	pend, ok := s.pendingSubs[id]
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
	if pend.short != "" {
		s.shortSubs[pend.short] = struct{}{}
	}
	s.gw.sendTo(s.addr, mqttsn.Suback{QoS: e.GrantedQoS, TopicID: pend.topicID, MsgID: id, ReturnCode: mqttsn.ReturnAccepted})
}

// handleUnsubscribe forwards an UNSUBSCRIBE of a normal topic name or filter, or of a short name, to the broker.
// The client's topic table entry is kept, because its topic id stays valid for publishing. A short name is removed
// from shortSubs when the broker acknowledges the UNSUBSCRIBE. Predefined topic ids, and short names that are not
// two characters, are acknowledged at once without broker contact, since UNSUBACK has no return code. A request
// whose msg id is still pending is dropped silently, since UNSUBACK cannot report a refusal. An error means the
// broker write failed.
func (s *session) handleUnsubscribe(broker *mqtt.Client, m mqttsn.Unsubscribe, log *slog.Logger) error {
	if m.MsgID == 0 {
		log.Debug("dropped UNSUBSCRIBE with msg id 0")
		return nil
	}
	var short string
	switch {
	case m.TopicIDType == mqttsn.TopicIDNormal:
	case m.TopicIDType == mqttsn.TopicIDShort:
		if _, ok := shortID(m.TopicName); !ok {
			log.Warn("UNSUBSCRIBE with invalid short topic name acknowledged without broker contact")
			s.gw.sendTo(s.addr, mqttsn.Unsuback{MsgID: m.MsgID})
			return nil
		}
		short = m.TopicName
	default:
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
	s.pendingUnsubs[m.MsgID] = short
	return nil
}

// handleUnsuback answers the client's UNSUBSCRIBE with the same msg id, and forgets a short name it removed. An
// unknown packet id is logged and ignored.
func (s *session) handleUnsuback(e mqtt.Event, log *slog.Logger) {
	id := e.PacketID
	short, ok := s.pendingUnsubs[id]
	if !ok {
		log.Warn("ignored broker UNSUBACK for unknown packet id", "packet_id", id)
		return
	}
	delete(s.pendingUnsubs, id)
	if short != "" {
		delete(s.shortSubs, short)
	}
	s.gw.sendTo(s.addr, mqttsn.Unsuback{MsgID: id})
}
