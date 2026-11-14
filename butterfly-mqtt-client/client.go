// Package mqtt is a minimal MQTT 3.1.1 client: it connects over TCP, sends CONNECT, DISCONNECT,
// QoS 0 and QoS 1 PUBLISH, PINGREQ and SUBSCRIBE (QoS 0), and reads CONNACK, PUBACK, PINGRESP,
// SUBACK, DISCONNECT and QoS 0 PUBLISH. It has no knowledge of any gateway or other protocol.
package mqtt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	protocolLevel311 = 4
	writeTimeout     = 5 * time.Second
	flagCleanSession = 0x02
	flagWill         = 0x04
	willQoSShift     = 3
	flagWillRetain   = 0x20
)

// ErrInvalidWill reports will fields that cannot be sent: QoS above 1, or will fields without a will topic.
var ErrInvalidWill = errors.New("mqtt: invalid will")

// ConnectOptions holds the CONNECT fields. Username and password are not supported yet.
// A will is set only when WillTopic is not empty; then WillPayload, WillQoS (0 or 1) and WillRetain apply.
// With an empty WillTopic, the other will fields must be zero.
type ConnectOptions struct {
	ClientID     string
	CleanSession bool
	KeepAlive    uint16 // seconds; 0 disables the broker's keepalive check
	WillTopic    string
	WillPayload  []byte
	WillQoS      byte
	WillRetain   bool
}

// ConnackError is returned by Connect when the broker answers with a non-zero return code.
type ConnackError struct {
	Code byte
}

func (e *ConnackError) Error() string {
	return fmt.Sprintf("mqtt: broker refused connection, return code %d", e.Code)
}

// Client is one TCP connection to a broker. Close and Disconnect are safe to call concurrently.
type Client struct {
	conn      net.Conn
	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
	packetID  uint16 // last SUBSCRIBE packet identifier; only used by Subscribe
}

// Dial opens a TCP connection to addr ("host:port"), bounded by ctx and timeout.
func Dial(ctx context.Context, addr string, timeout time.Duration) (*Client, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// Connect sends CONNECT and waits for CONNACK. The deadline and cancellation of ctx bound the
// exchange, so callers should pass a context with a timeout. A non-zero return code yields a *ConnackError.
func (c *Client) Connect(ctx context.Context, opts ConnectOptions) error {
	body, err := connectBody(opts)
	if err != nil {
		return err
	}
	pkt, err := packet(typeConnect, 0, body)
	if err != nil {
		return err
	}

	deadline, _ := ctx.Deadline()
	if err := c.conn.SetDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()

	if err := c.write(pkt); err != nil {
		return ctxErr(ctx, err)
	}
	p, err := ReadPacket(c.conn)
	if err != nil {
		return ctxErr(ctx, err)
	}
	if err := c.conn.SetDeadline(time.Time{}); err != nil {
		return err
	}
	return checkConnack(p)
}

// Disconnect sends DISCONNECT and closes the connection.
func (c *Client) Disconnect() error {
	pkt, err := packet(typeDisconnect, 0, nil)
	if err != nil {
		return err
	}
	writeErr := c.write(pkt)
	closeErr := c.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// Close closes the TCP connection without sending DISCONNECT. Repeated calls return the first result.
func (c *Client) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.conn.Close() })
	return c.closeErr
}

func (c *Client) write(pkt []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	_, err := c.conn.Write(pkt)
	return err
}

func ctxErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func connectBody(o ConnectOptions) ([]byte, error) {
	var b []byte
	b, err := appendString(b, "MQTT")
	if err != nil {
		return nil, err
	}
	flags := byte(0)
	if o.CleanSession {
		flags |= flagCleanSession
	}
	if o.WillTopic != "" {
		if o.WillQoS > 1 {
			return nil, fmt.Errorf("%w: QoS %d, want 0 or 1", ErrInvalidWill, o.WillQoS)
		}
		if strings.ContainsAny(o.WillTopic, "+#") {
			return nil, ErrInvalidTopic
		}
		flags |= flagWill | o.WillQoS<<willQoSShift
		if o.WillRetain {
			flags |= flagWillRetain
		}
	} else if len(o.WillPayload) > 0 || o.WillQoS != 0 || o.WillRetain {
		return nil, fmt.Errorf("%w: will fields without a will topic", ErrInvalidWill)
	}
	b = append(b, protocolLevel311, flags, byte(o.KeepAlive>>8), byte(o.KeepAlive))
	b, err = appendString(b, o.ClientID)
	if err != nil {
		return nil, err
	}
	if o.WillTopic == "" {
		return b, nil
	}
	b, err = appendString(b, o.WillTopic)
	if err != nil {
		return nil, err
	}
	return appendBytes(b, o.WillPayload)
}

func checkConnack(p Packet) error {
	if p.Type != typeConnack || len(p.Body) != 2 || p.Body[0]&0xFE != 0 {
		return fmt.Errorf("%w: expected CONNACK", ErrMalformedPacket)
	}
	if p.Body[1] != 0 {
		return &ConnackError{Code: p.Body[1]}
	}
	return nil
}
