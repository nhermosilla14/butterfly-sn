package gateway

import (
	"context"
	"log/slog"
	"net"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// session owns one MQTT-SN client and its broker connection. Only run touches session state.
type session struct {
	gw     *Gateway
	key    string
	addr   *net.UDPAddr
	inbox  chan mqttsn.Message
	topics topicTable
}

// deliver hands a datagram to the session without blocking the router.
func (s *session) deliver(m mqttsn.Message) {
	select {
	case s.inbox <- m:
	default:
		s.gw.log.Debug("session inbox full, dropping packet", "from", s.addr)
	}
}

func (s *session) run(ctx context.Context, c mqttsn.Connect) {
	defer s.gw.wg.Done()
	defer s.gw.removeSession(s)

	log := s.gw.log.With("client", s.addr.String(), "client_id", c.ClientID)
	if c.Will {
		log.Debug("CONNECT Will flag ignored: will is not supported yet")
	}

	broker, err := s.connectBroker(ctx, c)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Warn("broker connect failed, rejecting client", "broker", s.gw.cfg.Broker, "error", err)
		// Remove before replying so a client that retries at once gets a fresh session, not this one.
		s.gw.removeSession(s)
		s.gw.sendTo(s.addr, mqttsn.Connack{ReturnCode: mqttsn.ReturnCongestion})
		return
	}
	log.Info("client connected")
	s.gw.sendTo(s.addr, mqttsn.Connack{ReturnCode: mqttsn.ReturnAccepted})

	for {
		select {
		case <-ctx.Done():
			_ = broker.Disconnect()
			s.gw.sendTo(s.addr, mqttsn.Disconnect{})
			log.Info("client closed by shutdown")
			return
		case m := <-s.inbox:
			switch m := m.(type) {
			case mqttsn.Disconnect:
				_ = broker.Disconnect()
				s.gw.removeSession(s)
				s.gw.sendTo(s.addr, mqttsn.Disconnect{})
				log.Info("client disconnected")
				return
			case mqttsn.Register:
				s.handleRegister(m, log)
			case mqttsn.Publish:
				s.handlePublish(broker, m, log)
			default:
				log.Debug("ignored packet", "type", m)
			}
		}
	}
}

func (s *session) handleRegister(m mqttsn.Register, log *slog.Logger) {
	id, ok := s.topics.register(m.TopicName)
	code := byte(mqttsn.ReturnAccepted)
	if !ok {
		code = mqttsn.ReturnCongestion
		log.Warn("topic table full, rejecting REGISTER")
	}
	s.gw.sendTo(s.addr, mqttsn.Regack{TopicID: id, MsgID: m.MsgID, ReturnCode: code})
}

// handlePublish forwards QoS 0 publishes on normal topic ids. Other publishes are not handled yet.
// The payload is never logged.
func (s *session) handlePublish(broker *mqtt.Client, m mqttsn.Publish, log *slog.Logger) {
	if m.QoS != 0 || m.TopicIDType != mqttsn.TopicIDNormal {
		log.Debug("ignored PUBLISH: only QoS 0 with normal topic id is supported", "qos", m.QoS, "topic_id_type", m.TopicIDType)
		return
	}
	name, ok := s.topics.lookup(m.TopicID)
	if !ok {
		log.Debug("dropped PUBLISH with unknown topic id", "topic_id", m.TopicID)
		return
	}
	if err := broker.Publish(name, m.Data, m.Retain); err != nil {
		log.Warn("publish to broker failed", "topic", name, "error", err)
	}
}

func (s *session) connectBroker(ctx context.Context, c mqttsn.Connect) (*mqtt.Client, error) {
	cl, err := mqtt.Dial(ctx, s.gw.cfg.Broker, brokerTimeout)
	if err != nil {
		return nil, err
	}
	hctx, cancel := context.WithTimeout(ctx, brokerTimeout)
	defer cancel()
	err = cl.Connect(hctx, mqtt.ConnectOptions{ClientID: c.ClientID, CleanSession: true, KeepAlive: c.Duration})
	if err != nil {
		_ = cl.Close()
		return nil, err
	}
	return cl, nil
}
