// Package gateway runs the MQTT-SN UDP listener and routes datagrams to per-client sessions.
// Each session goroutine owns its state; the router only touches the session map, under a mutex.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/config"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

const (
	brokerTimeout = 5 * time.Second
	inboxSize     = 16
	maxDatagram   = 65535
	// maxDiscoveryDelay bounds the random wait before a GWINFO reply, so searching clients do not all answer at once.
	maxDiscoveryDelay = 500 * time.Millisecond
)

// discoveryDelay picks the wait before a GWINFO reply. Tests replace it to avoid sleeping.
var discoveryDelay = func() time.Duration { return rand.N(maxDiscoveryDelay) }

// Gateway is a UDP listener bound by New and served by Run.
type Gateway struct {
	cfg  config.Config
	log  *slog.Logger
	conn *net.UDPConn
	wg   sync.WaitGroup

	mu       sync.Mutex
	sessions map[string]*session // keyed by sender address
}

// New binds the UDP listen address from cfg. Use Addr to learn the bound address when the port is 0.
func New(cfg config.Config, log *slog.Logger) (*Gateway, error) {
	laddr, err := net.ResolveUDPAddr("udp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}
	return &Gateway{cfg: cfg, log: log, conn: conn, sessions: make(map[string]*session)}, nil
}

// Addr returns the bound UDP address.
func (g *Gateway) Addr() net.Addr {
	return g.conn.LocalAddr()
}

// Run routes datagrams until ctx is cancelled. Then every session ends its broker connection (MQTT
// DISCONNECT, or a bare close when it has a will), sends DISCONNECT to its client and ends, and the UDP
// socket is closed.
func (g *Gateway) Run(ctx context.Context) error {
	defer g.conn.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer g.wg.Wait()
	defer cancel()

	stop := context.AfterFunc(ctx, func() { _ = g.conn.SetReadDeadline(time.Unix(1, 0)) })
	defer stop()

	buf := make([]byte, maxDatagram)
	for {
		n, addr, err := g.conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("gateway: read udp: %w", err)
		}
		g.route(ctx, buf[:n], addr)
	}
}

// route handles one datagram. A valid packet of a type not implemented yet has msg nil and err ErrUnsupported.
func (g *Gateway) route(ctx context.Context, data []byte, addr *net.UDPAddr) {
	msg, err := mqttsn.Decode(data)
	if err != nil && !errors.Is(err, mqttsn.ErrUnsupported) {
		g.log.Debug("ignored malformed datagram", "from", addr, "error", err)
		return
	}
	// Discovery is handled before session lookup, so it never creates or touches a session.
	switch msg.(type) {
	case mqttsn.Searchgw:
		g.scheduleGwinfo(ctx, addr)
		return
	case mqttsn.Gwinfo, mqttsn.Advertise:
		g.log.Debug("ignored discovery message", "from", addr, "type", fmt.Sprintf("%T", msg))
		return
	}
	key := addr.String()

	g.mu.Lock()
	s := g.sessions[key]
	if s == nil {
		g.openSessionLocked(ctx, key, addr, msg)
		g.mu.Unlock()
		return
	}
	g.mu.Unlock()
	// A nil msg (unsupported type) still reaches the session, where it counts as activity.
	s.deliver(msg)
}

// scheduleGwinfo sends one unicast GWINFO to addr after a random delay. The radius of SEARCHGW is ignored.
// The wait is part of g.wg and ends when ctx does, so Run closes the socket only after no reply is pending.
// Searches are not rate-limited.
func (g *Gateway) scheduleGwinfo(ctx context.Context, addr *net.UDPAddr) {
	timer := time.NewTimer(discoveryDelay())
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		defer timer.Stop()
		select {
		case <-timer.C:
			g.sendTo(addr, mqttsn.Gwinfo{GwID: byte(g.cfg.GatewayID)})
		case <-ctx.Done():
		}
	}()
}

// openSessionLocked starts a session for a CONNECT from an unknown sender. Any other packet from an
// unknown sender gets DISCONNECT, except DISCONNECT itself, which is ignored. The caller holds g.mu.
func (g *Gateway) openSessionLocked(ctx context.Context, key string, addr *net.UDPAddr, msg mqttsn.Message) {
	c, ok := msg.(mqttsn.Connect)
	if !ok {
		if _, isDisconnect := msg.(mqttsn.Disconnect); isDisconnect {
			g.log.Debug("ignored DISCONNECT from unknown sender", "from", addr)
			return
		}
		g.log.Debug("packet from unknown sender, sending DISCONNECT", "from", addr, "type", fmt.Sprintf("%T", msg))
		g.sendTo(addr, mqttsn.Disconnect{})
		return
	}
	if c.ClientID == "" {
		g.log.Warn("CONNECT with empty client id rejected", "from", addr)
		g.sendTo(addr, mqttsn.Disconnect{})
		return
	}
	if len(g.sessions) >= g.cfg.MaxClients {
		g.log.Warn("max_clients reached, rejecting CONNECT", "from", addr, "max_clients", g.cfg.MaxClients)
		g.sendTo(addr, mqttsn.Connack{ReturnCode: mqttsn.ReturnCongestion})
		return
	}
	s := &session{gw: g, key: key, addr: addr, inbox: make(chan inbound, inboxSize)}
	g.sessions[key] = s
	g.wg.Add(1)
	go s.run(ctx, c)
}

func (g *Gateway) removeSession(s *session) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sessions[s.key] == s {
		delete(g.sessions, s.key)
	}
}

func (g *Gateway) sendTo(addr *net.UDPAddr, m mqttsn.Message) {
	if g.conn == nil {
		return
	}
	b, err := mqttsn.Encode(m)
	if err != nil {
		g.log.Error("encode failed", "error", err)
		return
	}
	if _, err := g.conn.WriteToUDP(b, addr); err != nil {
		g.log.Warn("send failed", "to", addr, "error", err)
	}
}
