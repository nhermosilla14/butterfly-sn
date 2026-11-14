// Package mqttsn encodes and decodes MQTT-SN v1.2 messages. It does no I/O.
package mqttsn

import (
	"errors"
	"fmt"
)

const (
	msgConnect    = 0x04
	msgConnack    = 0x05
	msgDisconnect = 0x18

	protocolID = 0x01

	flagWill         = 0x08
	flagCleanSession = 0x04
)

// Return codes carried by CONNACK.
const (
	ReturnAccepted       = 0x00
	ReturnCongestion     = 0x01
	ReturnInvalidTopicID = 0x02
	ReturnNotSupported   = 0x03
)

var (
	// ErrMalformed reports a packet whose length or body does not match its message type.
	ErrMalformed = errors.New("mqttsn: malformed message")
	// ErrUnsupported reports a valid header of a message type this package does not implement yet.
	ErrUnsupported = errors.New("mqttsn: unsupported message type")
)

// Message is one decoded MQTT-SN message: Connect, Connack or Disconnect.
type Message interface {
	isMessage()
}

// Connect is a CONNECT message. Will is reported only; the will exchange is not implemented.
type Connect struct {
	CleanSession bool
	Will         bool
	Duration     uint16
	ClientID     string
}

// Connack is a CONNACK message.
type Connack struct {
	ReturnCode byte
}

// Disconnect is a DISCONNECT message. Duration is present only when HasDuration is set (sleep request).
type Disconnect struct {
	Duration    uint16
	HasDuration bool
}

func (Connect) isMessage()    {}
func (Connack) isMessage()    {}
func (Disconnect) isMessage() {}

// Encode serialises m with its length header.
func Encode(m Message) ([]byte, error) {
	var typ byte
	var body []byte
	switch m := m.(type) {
	case Connect:
		typ = msgConnect
		var flags byte
		if m.CleanSession {
			flags |= flagCleanSession
		}
		if m.Will {
			flags |= flagWill
		}
		body = []byte{flags, protocolID, byte(m.Duration >> 8), byte(m.Duration)}
		body = append(body, m.ClientID...)
	case Connack:
		typ = msgConnack
		body = []byte{m.ReturnCode}
	case Disconnect:
		typ = msgDisconnect
		if m.HasDuration {
			body = []byte{byte(m.Duration >> 8), byte(m.Duration)}
		}
	default:
		return nil, fmt.Errorf("mqttsn: cannot encode %T", m)
	}
	return withHeader(typ, body)
}

func withHeader(typ byte, body []byte) ([]byte, error) {
	total := 2 + len(body)
	if total < 256 {
		return append([]byte{byte(total), typ}, body...), nil
	}
	total += 2
	if total > 0xFFFF {
		return nil, fmt.Errorf("mqttsn: message of %d bytes is too long", total)
	}
	out := []byte{0x01, byte(total >> 8), byte(total), typ}
	return append(out, body...), nil
}

// Decode parses one datagram. The declared length must match len(data) exactly.
func Decode(data []byte) (Message, error) {
	if len(data) < 2 {
		return nil, ErrMalformed
	}
	length, hdr := int(data[0]), 1
	if data[0] == 0x01 {
		if len(data) < 4 {
			return nil, ErrMalformed
		}
		length, hdr = int(data[1])<<8|int(data[2]), 3
	}
	if length != len(data) || length < hdr+1 {
		return nil, ErrMalformed
	}
	typ, body := data[hdr], data[hdr+1:]
	switch typ {
	case msgConnect:
		if len(body) < 4 || body[1] != protocolID {
			return nil, ErrMalformed
		}
		return Connect{
			CleanSession: body[0]&flagCleanSession != 0,
			Will:         body[0]&flagWill != 0,
			Duration:     uint16(body[2])<<8 | uint16(body[3]),
			ClientID:     string(body[4:]),
		}, nil
	case msgConnack:
		if len(body) != 1 {
			return nil, ErrMalformed
		}
		return Connack{ReturnCode: body[0]}, nil
	case msgDisconnect:
		switch len(body) {
		case 0:
			return Disconnect{}, nil
		case 2:
			return Disconnect{Duration: uint16(body[0])<<8 | uint16(body[1]), HasDuration: true}, nil
		}
		return nil, ErrMalformed
	}
	return nil, ErrUnsupported
}
