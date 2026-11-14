package gateway

import (
	"context"
	"log/slog"
	"net"
	"sync"

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
	// pending maps the msg id of each QoS 1 PUBLISH still awaiting the broker's PUBACK to its topic id.
	// The broker packet id is the client's msg id, so no translation is needed.
	pending map[uint16]uint16
}

// brokerAck is one result read from the broker connection: a PUBACK packet id, or the read error that
// ended the reader.
type brokerAck struct {
	id  uint16
	err error
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
	s.pending = make(map[uint16]uint16)
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

	// The reader ends when the connection is closed (below) or its context is cancelled. Deferred
	// calls run in reverse: close the connection, cancel the reader and wait for it to return.
	readerCtx, stopReader := context.WithCancel(ctx)
	var readers sync.WaitGroup
	defer readers.Wait()
	defer stopReader()
	defer broker.Close()
	acks := make(chan brokerAck)
	readers.Add(1)
	go s.readBroker(readerCtx, broker, acks, &readers)

	for {
		select {
		case <-ctx.Done():
			_ = broker.Disconnect()
			s.gw.sendTo(s.addr, mqttsn.Disconnect{})
			log.Info("client closed by shutdown")
			return
		case a := <-acks:
			s.handleBrokerAck(a, log)
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

// readBroker reads PUBACKs from the broker and hands them to the session loop. It ends after a read
// error, which it sends first, or when ctx is cancelled.
func (s *session) readBroker(ctx context.Context, broker *mqtt.Client, acks chan<- brokerAck, readers *sync.WaitGroup) {
	defer readers.Done()
	for {
		id, err := broker.ReadPuback(ctx)
		select {
		case acks <- brokerAck{id: id, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// handleBrokerAck sends the client its PUBACK once the broker has acknowledged the matching PUBLISH.
func (s *session) handleBrokerAck(a brokerAck, log *slog.Logger) {
	if a.err != nil {
		log.Warn("broker connection read failed, no more broker PUBACKs will be read", "error", a.err)
		return
	}
	topicID, ok := s.pending[a.id]
	if !ok {
		log.Warn("ignored broker PUBACK for unknown packet id", "packet_id", a.id)
		return
	}
	delete(s.pending, a.id)
	s.gw.sendTo(s.addr, mqttsn.Puback{TopicID: topicID, MsgID: a.id, ReturnCode: mqttsn.ReturnAccepted})
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

// handlePublish forwards QoS 0 and QoS 1 publishes on normal topic ids. Other publishes are not handled yet.
// The payload is never logged.
func (s *session) handlePublish(broker *mqtt.Client, m mqttsn.Publish, log *slog.Logger) {
	if m.QoS > 1 || m.TopicIDType != mqttsn.TopicIDNormal {
		log.Debug("ignored PUBLISH: only QoS 0 and 1 with normal topic id are supported", "qos", m.QoS, "topic_id_type", m.TopicIDType)
		return
	}
	name, ok := s.topics.lookup(m.TopicID)
	if !ok {
		log.Debug("dropped PUBLISH with unknown topic id", "topic_id", m.TopicID)
		return
	}
	if m.QoS == 1 {
		s.publishQoS1(broker, m, name, log)
		return
	}
	if err := broker.Publish(name, m.Data, m.Retain); err != nil {
		log.Warn("publish to broker failed", "topic", name, "error", err)
	}
}

// publishQoS1 sends the PUBLISH to the broker. The client's PUBACK follows only once the broker acknowledges it.
// The session loop handles broker acks, so pending is updated before any ack can be read.
func (s *session) publishQoS1(broker *mqtt.Client, m mqttsn.Publish, name string, log *slog.Logger) {
	if m.MsgID == 0 {
		log.Debug("dropped QoS 1 PUBLISH with msg id 0")
		return
	}
	if _, busy := s.pending[m.MsgID]; busy {
		log.Warn("dropped QoS 1 PUBLISH: msg id still awaiting broker PUBACK", "msg_id", m.MsgID)
		return
	}
	if err := broker.PublishQoS1(name, m.Data, m.Retain, m.DUP, m.MsgID); err != nil {
		log.Warn("QoS 1 publish to broker failed", "topic", name, "error", err)
		return
	}
	s.pending[m.MsgID] = m.TopicID
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
