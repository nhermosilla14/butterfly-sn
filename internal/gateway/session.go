package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

var (
	errBrokerDisconnect = errors.New("broker sent MQTT DISCONNECT")
	errPingTimeout      = errors.New("broker PINGRESP missing")
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
	// lastWrite is when the broker connection was last written successfully (CONNECT, PUBLISH, PINGREQ).
	// Reads do not count. Only the session goroutine touches it.
	lastWrite time.Time
	// pingOutstanding is set while a PINGREQ awaits its PINGRESP.
	pingOutstanding bool
}

// brokerEvent is one result read from the broker connection: a broker packet, or the read error that
// ended the reader.
type brokerEvent struct {
	event mqtt.Event
	err   error
}

// keepaliveInterval is how long the broker may stay silent before the gateway pings it: half the client's
// CONNECT duration, so the ping goes out well before the broker's 1.5x limit. Zero disables pings.
func keepaliveInterval(duration uint16) time.Duration {
	return time.Duration(duration) * time.Second / 2
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
	events := make(chan brokerEvent)
	readers.Add(1)
	go s.readBroker(readerCtx, broker, events, &readers)

	// The keepalive timer is armed only when the client asked for one; a nil channel never fires.
	interval := keepaliveInterval(c.Duration)
	var keepalive *time.Timer
	var keepaliveDue <-chan time.Time
	if interval > 0 {
		keepalive = time.NewTimer(interval)
		defer keepalive.Stop()
		keepaliveDue = keepalive.C
	}

	for {
		select {
		case <-ctx.Done():
			_ = broker.Disconnect()
			s.gw.sendTo(s.addr, mqttsn.Disconnect{})
			log.Info("client closed by shutdown")
			return
		case e := <-events:
			if ctx.Err() != nil {
				continue // shutdown: the case above ends the session
			}
			if e.err != nil {
				s.loseBroker(log, e.err)
				return
			}
			if e.event.Kind == mqtt.EventDisconnect {
				s.loseBroker(log, errBrokerDisconnect)
				return
			}
			s.handleBrokerEvent(e.event, log)
		case <-keepaliveDue:
			if err := s.keepaliveDue(broker, interval, keepalive); err != nil {
				s.loseBroker(log, err)
				return
			}
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
				if err := s.handlePublish(broker, m, log); err != nil {
					s.loseBroker(log, err)
					return
				}
			default:
				log.Debug("ignored packet", "type", m)
			}
		}
	}
}

// loseBroker ends the session after the broker connection is lost. The client gets one DISCONNECT, sent after
// the session is removed so a retry from the client gets a fresh session. The caller then returns, which closes
// the broker connection and joins the reader. Pending QoS 1 ids are dropped without PUBACK. The cause is logged
// without payload bytes.
func (s *session) loseBroker(log *slog.Logger, err error) {
	log.Warn("broker connection lost, disconnecting client", "error", err)
	s.gw.removeSession(s)
	s.gw.sendTo(s.addr, mqttsn.Disconnect{})
}

// readBroker reads broker packets in arrival order and hands them to the session loop. It ends after a
// read error or an MQTT DISCONNECT, which it sends first, or when ctx is cancelled.
func (s *session) readBroker(ctx context.Context, broker *mqtt.Client, events chan<- brokerEvent, readers *sync.WaitGroup) {
	defer readers.Done()
	for {
		ev, err := broker.ReadEvent(ctx)
		select {
		case events <- brokerEvent{event: ev, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil || ev.Kind == mqtt.EventDisconnect {
			return
		}
	}
}

// handleBrokerEvent handles one broker packet. A PUBACK is sent to the client once the broker has
// acknowledged the matching PUBLISH.
func (s *session) handleBrokerEvent(e mqtt.Event, log *slog.Logger) {
	switch e.Kind {
	case mqtt.EventPuback:
		id := e.PacketID
		topicID, ok := s.pending[id]
		if !ok {
			log.Warn("ignored broker PUBACK for unknown packet id", "packet_id", id)
			return
		}
		delete(s.pending, id)
		s.gw.sendTo(s.addr, mqttsn.Puback{TopicID: topicID, MsgID: id, ReturnCode: mqttsn.ReturnAccepted})
	case mqtt.EventPingresp:
		s.pingOutstanding = false
	}
}

// keepaliveDue runs when the keepalive timer fires. It pings the broker if nothing was written for a whole
// interval and re-arms the timer. A PINGREQ still unanswered when the next one is due means the broker is lost.
func (s *session) keepaliveDue(broker *mqtt.Client, interval time.Duration, timer *time.Timer) error {
	if s.pingOutstanding {
		return errPingTimeout
	}
	if idle := time.Since(s.lastWrite); idle < interval {
		timer.Reset(interval - idle)
		return nil
	}
	timer.Reset(interval)
	if err := broker.Pingreq(); err != nil {
		return fmt.Errorf("keepalive PINGREQ: %w", err)
	}
	s.lastWrite = time.Now()
	s.pingOutstanding = true
	return nil
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
// The payload is never logged. An error means the broker write failed and the broker connection is lost.
func (s *session) handlePublish(broker *mqtt.Client, m mqttsn.Publish, log *slog.Logger) error {
	if m.QoS > 1 || m.TopicIDType != mqttsn.TopicIDNormal {
		log.Debug("ignored PUBLISH: only QoS 0 and 1 with normal topic id are supported", "qos", m.QoS, "topic_id_type", m.TopicIDType)
		return nil
	}
	name, ok := s.topics.lookup(m.TopicID)
	if !ok {
		log.Debug("dropped PUBLISH with unknown topic id", "topic_id", m.TopicID)
		return nil
	}
	if m.QoS == 1 {
		return s.publishQoS1(broker, m, name, log)
	}
	if err := broker.Publish(name, m.Data, m.Retain); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	s.lastWrite = time.Now()
	return nil
}

// publishQoS1 sends the PUBLISH to the broker. The client's PUBACK follows only once the broker acknowledges it.
// The session loop handles broker acks, so pending is updated before any ack can be read.
func (s *session) publishQoS1(broker *mqtt.Client, m mqttsn.Publish, name string, log *slog.Logger) error {
	if m.MsgID == 0 {
		log.Debug("dropped QoS 1 PUBLISH with msg id 0")
		return nil
	}
	if _, busy := s.pending[m.MsgID]; busy {
		log.Warn("dropped QoS 1 PUBLISH: msg id still awaiting broker PUBACK", "msg_id", m.MsgID)
		return nil
	}
	if err := broker.PublishQoS1(name, m.Data, m.Retain, m.DUP, m.MsgID); err != nil {
		return fmt.Errorf("QoS 1 publish: %w", err)
	}
	s.lastWrite = time.Now()
	s.pending[m.MsgID] = m.TopicID
	return nil
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
	s.lastWrite = time.Now()
	return cl, nil
}
