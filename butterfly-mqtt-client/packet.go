package mqtt

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

const (
	typeConnect    = 1
	typeConnack    = 2
	typeDisconnect = 14
)

// ErrMalformedPacket reports a fixed header or body that breaks the MQTT 3.1.1 framing rules.
var ErrMalformedPacket = errors.New("mqtt: malformed packet")

// Packet is one framed MQTT control packet read from a stream.
type Packet struct {
	Type  byte
	Flags byte
	Body  []byte
}

// ReadPacket reads exactly one control packet from r: the fixed header, the variable-length
// remaining length and then exactly that many body bytes.
func ReadPacket(r io.Reader) (Packet, error) {
	var first [1]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		return Packet{}, err
	}
	remaining, err := readRemainingLength(r)
	if err != nil {
		return Packet{}, err
	}
	// Copy through a buffer so a huge declared length cannot force a huge allocation up front.
	var body bytes.Buffer
	if _, err := io.CopyN(&body, r, int64(remaining)); err != nil {
		return Packet{}, err
	}
	return Packet{Type: first[0] >> 4, Flags: first[0] & 0x0F, Body: body.Bytes()}, nil
}

func readRemainingLength(r io.Reader) (int, error) {
	var b [1]byte
	value, multiplier := 0, 1
	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
		value += int(b[0]&0x7F) * multiplier
		if b[0]&0x80 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, fmt.Errorf("%w: remaining length longer than 4 bytes", ErrMalformedPacket)
}

// packet builds a full control packet: fixed header with the variable-length remaining length, then body.
func packet(typ, flags byte, body []byte) ([]byte, error) {
	if len(body) > 268435455 {
		return nil, fmt.Errorf("%w: body of %d bytes exceeds the MQTT limit", ErrMalformedPacket, len(body))
	}
	out := []byte{typ<<4 | flags}
	n := len(body)
	for {
		digit := byte(n % 128)
		n /= 128
		if n > 0 {
			digit |= 0x80
		}
		out = append(out, digit)
		if n == 0 {
			break
		}
	}
	return append(out, body...), nil
}

func appendString(b []byte, s string) ([]byte, error) {
	if len(s) > 0xFFFF {
		return nil, fmt.Errorf("%w: string of %d bytes exceeds 65535", ErrMalformedPacket, len(s))
	}
	b = append(b, byte(len(s)>>8), byte(len(s)))
	return append(b, s...), nil
}
