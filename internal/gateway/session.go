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
	inbox  chan inbound
	topics topicTable
	// pending maps the msg id of each QoS 1 PUBLISH still awaiting the broker's PUBACK to its topic id.
	// The broker packet id is the client's msg id, so no translation is needed.
	pending map[uint16]uint16
	// pendingSubs maps the msg id of each SUBSCRIBE awaiting the broker's SUBACK to what it reports. See pendingSub.
	pendingSubs map[uint16]pendingSub
	// pendingUnsubs maps the msg id of each UNSUBSCRIBE awaiting the broker's UNSUBACK to its short name, or to
	// "" for a normal one.
	pendingUnsubs map[uint16]string
	// shortSubs holds the two-character short names this session is subscribed to. They do not use the table.
	shortSubs map[string]struct{}
	// regMsgID is the last gateway REGISTER msg id issued; see nextRegMsgID.
	regMsgID uint16
	// pendingRegs maps the msg id of each gateway REGISTER awaiting REGACK to its held PUBLISHes, and
	// regByTopic maps each topic awaiting REGACK to that msg id. See downlink.go.
	pendingRegs map[uint16]*pendingReg
	regByTopic  map[string]uint16
	// regSeq is the last creation order given to a pendingReg. See pendingReg.seq.
	regSeq uint64
	// pendingDownlink maps the broker packet id of each QoS 1 PUBLISH sent to the client, awaiting the
	// client's PUBACK, to the topic id sent. The broker is acknowledged only after that PUBACK.
	pendingDownlink map[uint16]uint16
	// lastWrite is when the broker connection was last written successfully (CONNECT, PUBLISH, PINGREQ).
	// Reads do not count. Only the session goroutine touches it.
	lastWrite time.Time
	// pingOutstanding is set while a PINGREQ awaits its PINGRESP.
	pingOutstanding bool
	// will is the client's will, collected by awaitWill before the broker is dialled. It is never logged.
	will willState
	// sleeping is set while the client sleeps after a DISCONNECT with a duration. See sleep.go.
	sleeping bool
	// sleepDuration is the duration the client asked for when it went to sleep. The sleep timer uses 1.5 times it.
	sleepDuration uint16
	// sleepBuf holds the broker PUBLISHes buffered while the client sleeps or is flushed, in arrival order.
	sleepBuf []mqtt.Event
	// flushing is set while the buffer is delivered after a wake. Broker PUBLISHes keep being buffered meanwhile.
	flushing bool
	// flushPing is set when the flush was started by PINGREQ: PINGRESP ends it and the client sleeps again. A
	// flush started by CONNECT leaves the client active.
	flushPing bool
	// flushMsgID is the last gateway msg id given to a buffered QoS 1 PUBLISH; see nextFlushMsgID.
	flushMsgID uint16
	// preAcked holds the gateway msg ids of the QoS 1 PUBLISHes sent on wake. The broker was acknowledged when
	// each one was buffered, so the client's PUBACK for these must not produce a second broker PUBACK.
	preAcked map[uint16]struct{}
	// retries holds the gateway REGISTERs and QoS 1 PUBLISHes to the client awaiting their replies, and
	// retryTimer fires at the earliest due one. See retry.go.
	retries    map[retryKey]*retryEntry
	retryTimer *time.Timer
	// parked holds the QoS 1 PUBLISHes awaiting PUBACK while the client sleeps. See clearRetries.
	parked map[retryKey]mqttsn.Message
}

// willState is the will a client sent in the handshake. hasWill is set only once the will message arrived;
// a WILLTOPIC alone does not make a will.
type willState struct {
	hasWill bool
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

// willHandshakeTimeout bounds the whole will handshake, from WILLTOPICREQ to the last will prompt answer.
const willHandshakeTimeout = 10 * time.Second

// inbound is one client datagram handed to the session. msg is nil for a valid type this package does not
// decode; it still counts as client activity. at is the arrival time, used by the inactivity timer.
type inbound struct {
	msg mqttsn.Message
	at  time.Time
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

// inactivityLimit is how long the client may stay silent before it is declared lost: 1.5 times its CONNECT
// duration. Zero disables the inactivity timer.
func inactivityLimit(duration uint16) time.Duration {
	return time.Duration(duration) * time.Second * 3 / 2
}

// deliver hands a datagram to the session without blocking the router.
func (s *session) deliver(m mqttsn.Message) {
	select {
	case s.inbox <- inbound{msg: m, at: time.Now()}:
	default:
		s.gw.log.Debug("session inbox full, dropping packet", "from", s.addr)
	}
}

func (s *session) run(ctx context.Context, c mqttsn.Connect) {
	defer s.gw.wg.Done()
	defer s.gw.removeSession(s)

	log := s.gw.log.With("client", s.addr.String(), "client_id", c.ClientID)
	s.pending = make(map[uint16]uint16)
	s.pendingSubs = make(map[uint16]pendingSub)
	s.pendingUnsubs = make(map[uint16]string)
	s.shortSubs = make(map[string]struct{})
	s.pendingRegs = make(map[uint16]*pendingReg)
	s.regByTopic = make(map[string]uint16)
	s.pendingDownlink = make(map[uint16]uint16)
	s.preAcked = make(map[uint16]struct{})
	s.retries = make(map[retryKey]*retryEntry)
	s.parked = make(map[retryKey]mqttsn.Message)
	defer s.stopRetryTimer()
	if c.Will && !s.awaitWill(ctx, log) {
		return
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
	// The client's silence is counted from the CONNACK, so broker dialing does not use up its allowance.
	lastClient := time.Now()

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

	// The inactivity timer is armed only when the client asked for a keepalive; a nil channel never fires.
	limit := inactivityLimit(c.Duration)
	var inactivity *time.Timer
	var inactivityDue <-chan time.Time
	if limit > 0 {
		inactivity = time.NewTimer(limit)
		defer inactivity.Stop()
		inactivityDue = inactivity.C
	}

	// The sleep timer is armed only while the client sleeps; a nil channel never fires. Each arm uses a new
	// timer, so a stopped timer that already fired cannot leak an old expiry.
	var sleep *time.Timer
	var sleepDue <-chan time.Time
	disarmSleep := func() {
		if sleep != nil {
			sleep.Stop()
		}
		sleepDue = nil
	}
	armSleep := func() {
		disarmSleep()
		sleep = time.NewTimer(inactivityLimit(s.sleepDuration))
		sleepDue = sleep.C
	}
	defer disarmSleep()
	// runFlush delivers the buffer after a wake. When the flush ends, a PINGREQ wake sends PINGRESP and sleeps again.
	runFlush := func() {
		if !s.flushing || !s.advanceFlush(log) {
			return
		}
		s.flushing = false
		if s.flushPing {
			s.gw.sendTo(s.addr, mqttsn.Pingresp{})
			s.clearRetries()
			armSleep()
			log.Info("client asleep again")
		}
	}

	for {
		retryDue := s.armRetry()
		select {
		case <-ctx.Done():
			endBrokerConn(broker, s.will.hasWill)
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
			if err := s.handleBrokerEvent(broker, e.event, log); err != nil {
				s.loseBroker(log, err)
				return
			}
		case <-keepaliveDue:
			if err := s.keepaliveDue(broker, interval, keepalive); err != nil {
				s.loseBroker(log, err)
				return
			}
		case <-inactivityDue:
			// Packets queued before the deadline are handled first, so they can still refresh lastClient.
			if len(s.inbox) > 0 {
				inactivity.Reset(0)
				continue
			}
			if idle := time.Since(lastClient); idle < limit {
				inactivity.Reset(limit - idle)
				continue
			}
			log.Warn("client silent beyond its keepalive, disconnecting")
			s.clientTimedOut(broker)
			return
		case <-sleepDue:
			log.Warn("client asleep beyond its sleep duration, disconnecting")
			s.clientTimedOut(broker)
			return
		case <-retryDue:
			if s.retransmitDue(log) {
				s.clientTimedOut(broker)
				return
			}
		case in := <-s.inbox:
			lastClient = in.at
			if s.sleeping && s.ignoredWhileAsleep(in.msg, log) {
				continue
			}
			switch m := in.msg.(type) {
			case nil:
				log.Debug("ignored unsupported packet")
			case mqttsn.Pingreq:
				if !s.sleeping {
					s.gw.sendTo(s.addr, mqttsn.Pingresp{})
					break
				}
				if len(s.sleepBuf) == 0 {
					s.gw.sendTo(s.addr, mqttsn.Pingresp{})
					armSleep()
					break
				}
				log.Info("client woke with PINGREQ", "buffered", len(s.sleepBuf))
				s.flushing, s.flushPing = true, true
				s.restoreParked()
				disarmSleep()
				runFlush()
			case mqttsn.Connect:
				if !s.sleeping {
					log.Debug("ignored packet", "type", m)
					break
				}
				// The CONNECT duration, clean-session flag and will are ignored: the broker session is kept.
				s.gw.sendTo(s.addr, mqttsn.Connack{ReturnCode: mqttsn.ReturnAccepted})
				log.Info("client woke with CONNECT", "buffered", len(s.sleepBuf))
				s.sleeping = false
				s.flushing, s.flushPing = true, false
				s.restoreParked()
				disarmSleep()
				if limit > 0 {
					inactivity = time.NewTimer(limit)
					inactivityDue = inactivity.C
				}
				runFlush()
			case mqttsn.Disconnect:
				if m.HasDuration && m.Duration > 0 && !s.sleeping && !s.flushing {
					// Sleep: the broker stays connected and the client gets its DISCONNECT as the reply.
					s.sleeping = true
					s.sleepDuration = m.Duration
					if err := s.holdToSleep(broker, log); err != nil {
						s.loseBroker(log, err)
						return
					}
					if inactivity != nil {
						inactivity.Stop()
					}
					inactivityDue = nil
					armSleep()
					s.gw.sendTo(s.addr, mqttsn.Disconnect{})
					log.Info("client asleep", "sleep_duration", m.Duration)
					continue
				}
				_ = broker.Disconnect()
				s.gw.removeSession(s)
				s.gw.sendTo(s.addr, mqttsn.Disconnect{})
				log.Info("client disconnected")
				return
			case mqttsn.Register:
				s.handleRegister(m, log)
			case mqttsn.Regack:
				if err := s.handleRegack(broker, m, log); err != nil {
					s.loseBroker(log, err)
					return
				}
				runFlush()
			case mqttsn.Puback:
				if err := s.handleClientPuback(broker, m, log); err != nil {
					s.loseBroker(log, err)
					return
				}
			case mqttsn.Publish:
				if err := s.handlePublish(broker, m, log); err != nil {
					s.loseBroker(log, err)
					return
				}
			case mqttsn.Subscribe:
				if err := s.handleSubscribe(broker, m, log); err != nil {
					s.loseBroker(log, err)
					return
				}
			case mqttsn.Unsubscribe:
				if err := s.handleUnsubscribe(broker, m, log); err != nil {
					s.loseBroker(log, err)
					return
				}
			default:
				log.Debug("ignored packet", "type", m)
			}
		}
	}
}

// awaitWill runs the will handshake before the broker is contacted: WILLTOPICREQ, then WILLTOPIC; a non-empty
// topic is followed by WILLMSGREQ and WILLMSG. It returns true when the broker may be dialled. It returns false
// when the session must end without broker contact: the client sent DISCONNECT, the handshake timed out, or the
// will QoS is refused. Those cases remove the session and reply here. On shutdown it just returns false.
func (s *session) awaitWill(ctx context.Context, log *slog.Logger) bool {
	s.gw.sendTo(s.addr, mqttsn.WillTopicReq{})
	timer := time.NewTimer(willHandshakeTimeout)
	defer timer.Stop()
	awaitingMsg := false
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			log.Warn("will handshake timed out, disconnecting client")
			s.gw.removeSession(s)
			s.gw.sendTo(s.addr, mqttsn.Disconnect{})
			return false
		case in := <-s.inbox:
			switch m := in.msg.(type) {
			case mqttsn.WillTopic:
				switch {
				case awaitingMsg:
					log.Debug("ignored repeated WILLTOPIC during will handshake")
				case m.Topic == "":
					log.Info("client sent an empty will topic, connecting without a will")
					return true
				case m.QoS > 1:
					log.Warn("will QoS not supported, rejecting client", "qos", m.QoS)
					// Remove before replying, as on every rejection, so a retry gets a fresh session.
					s.gw.removeSession(s)
					s.gw.sendTo(s.addr, mqttsn.Connack{ReturnCode: mqttsn.ReturnNotSupported})
					return false
				default:
					s.will = willState{topic: m.Topic, qos: m.QoS, retain: m.Retain}
					awaitingMsg = true
					s.gw.sendTo(s.addr, mqttsn.WillMsgReq{})
				}
			case mqttsn.WillMsg:
				if !awaitingMsg {
					log.Debug("ignored WILLMSG before WILLTOPIC")
					continue
				}
				s.will.payload = m.Payload
				s.will.hasWill = true
				return true
			case mqttsn.Disconnect:
				s.gw.removeSession(s)
				s.gw.sendTo(s.addr, mqttsn.Disconnect{})
				log.Info("client disconnected during will handshake")
				return false
			case nil:
				log.Debug("ignored unsupported packet during will handshake")
			default:
				log.Debug("ignored packet during will handshake", "type", m)
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

// clientTimedOut ends the session after the client has been silent for longer than its inactivity limit, or
// asleep for longer than its sleep limit. The session is removed first, so the slot is free and a retry from
// the client gets a fresh session. Then the broker connection is ended (see endBrokerConn), and a best-effort
// DISCONNECT goes to the client. The caller returns, which closes the broker connection.
func (s *session) clientTimedOut(broker *mqtt.Client) {
	s.gw.removeSession(s)
	endBrokerConn(broker, s.will.hasWill)
	s.gw.sendTo(s.addr, mqttsn.Disconnect{})
}

// endBrokerConn ends the broker connection for a session that is going away without the client's consent.
// With a will, the socket is closed without MQTT DISCONNECT, so the broker publishes the will. Without one,
// MQTT DISCONNECT is sent first.
func endBrokerConn(broker *mqtt.Client, hasWill bool) {
	if hasWill {
		_ = broker.Close()
		return
	}
	_ = broker.Disconnect()
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
// acknowledged the matching PUBLISH. An error means a write to the broker failed and the broker connection is lost.
func (s *session) handleBrokerEvent(broker *mqtt.Client, e mqtt.Event, log *slog.Logger) error {
	switch e.Kind {
	case mqtt.EventPuback:
		id := e.PacketID
		topicID, ok := s.pending[id]
		if !ok {
			log.Warn("ignored broker PUBACK for unknown packet id", "packet_id", id)
			return nil
		}
		delete(s.pending, id)
		s.gw.sendTo(s.addr, mqttsn.Puback{TopicID: topicID, MsgID: id, ReturnCode: mqttsn.ReturnAccepted})
	case mqtt.EventSuback:
		s.handleSuback(e, log)
	case mqtt.EventUnsuback:
		s.handleUnsuback(e, log)
	case mqtt.EventPublish:
		if s.sleeping || s.flushing {
			return s.bufferDownlink(broker, e, log)
		}
		return s.handleDownlink(broker, e, log)
	case mqtt.EventPingresp:
		s.pingOutstanding = false
	}
	return nil
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

// handleRegister adds a normal topic name to the table. A name the broker cannot take (empty or with a wildcard)
// is refused with REGACK Invalid topic id and adds nothing.
func (s *session) handleRegister(m mqttsn.Register, log *slog.Logger) {
	if !isPublishableName(m.TopicName) {
		log.Warn("refused REGISTER: invalid topic name")
		s.gw.sendTo(s.addr, mqttsn.Regack{MsgID: m.MsgID, ReturnCode: mqttsn.ReturnInvalidTopicID})
		return
	}
	id, ok := s.topics.register(m.TopicName)
	code := byte(mqttsn.ReturnAccepted)
	if !ok {
		code = mqttsn.ReturnCongestion
		log.Warn("topic table full, rejecting REGISTER")
	}
	s.gw.sendTo(s.addr, mqttsn.Regack{TopicID: id, MsgID: m.MsgID, ReturnCode: code})
}

// handlePublish forwards QoS 0 and QoS 1 publishes on normal or short topic ids. Predefined publishes are not
// handled yet. The payload is never logged. An error means the broker write failed and the broker connection is lost.
func (s *session) handlePublish(broker *mqtt.Client, m mqttsn.Publish, log *slog.Logger) error {
	if m.QoS > 1 || (m.TopicIDType != mqttsn.TopicIDNormal && m.TopicIDType != mqttsn.TopicIDShort) {
		log.Debug("ignored PUBLISH: only QoS 0 and 1 with normal or short topic id are supported", "qos", m.QoS, "topic_id_type", m.TopicIDType)
		return nil
	}
	name, ok := s.publishTopic(m)
	if !ok {
		log.Debug("dropped PUBLISH with unknown topic id", "topic_id", m.TopicID)
		return nil
	}
	if !isPublishableName(name) {
		log.Debug("dropped PUBLISH with invalid topic name", "topic_id_type", m.TopicIDType)
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

// publishTopic returns the topic name of an uplink PUBLISH: the table entry for a normal id, or the two bytes of
// a short id. ok is false for an unknown id or an invalid short name.
func (s *session) publishTopic(m mqttsn.Publish) (string, bool) {
	if m.TopicIDType == mqttsn.TopicIDShort {
		return shortName(m.TopicID)
	}
	return s.topics.lookup(m.TopicID)
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
	opts := mqtt.ConnectOptions{ClientID: c.ClientID, CleanSession: true, KeepAlive: c.Duration}
	if s.will.hasWill {
		opts.WillTopic = s.will.topic
		opts.WillPayload = s.will.payload
		opts.WillQoS = s.will.qos
		opts.WillRetain = s.will.retain
	}
	hctx, cancel := context.WithTimeout(ctx, brokerTimeout)
	defer cancel()
	err = cl.Connect(hctx, opts)
	if err != nil {
		_ = cl.Close()
		return nil, err
	}
	s.lastWrite = time.Now()
	return cl, nil
}
