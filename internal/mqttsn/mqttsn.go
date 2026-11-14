// Package mqttsn encodes and decodes MQTT-SN v1.2 messages. It does no I/O.
package mqttsn

import (
	"errors"
	"fmt"
)

const (
	msgConnect    = 0x04
	msgConnack    = 0x05
	msgRegister   = 0x0A
	msgRegack     = 0x0B
	msgPublish    = 0x0C
	msgDisconnect = 0x18

	protocolID = 0x01

	flagWill         = 0x08
	flagCleanSession = 0x04

	flagDUP    = 0x80
	flagRetain = 0x10
	qosShift   = 5
	qosMask    = 0x03
	topicMask  = 0x03
)

// TopicIDType values carried in the PUBLISH flags.
const (
	TopicIDNormal     = 0x00
	TopicIDPredefined = 0x01
	TopicIDShort      = 0x02
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

// Message is one decoded MQTT-SN message: Connect, Connack, Register, Regack, Publish or Disconnect.
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

// Register is a REGISTER message. TopicID is 0 in client requests.
type Register struct {
	TopicID   uint16
	MsgID     uint16
	TopicName string
}

// Regack is a REGACK message answering a Register with the same MsgID.
type Regack struct {
	TopicID    uint16
	MsgID      uint16
	ReturnCode byte
}

// Publish is a PUBLISH message. QoS is 0 to 3 (3 is the MQTT-SN level -1). MsgID is only meaningful
// for QoS 1 and 2. Data may be any bytes, including none.
type Publish struct {
	DUP         bool
	QoS         byte
	Retain      bool
	TopicIDType byte
	TopicID     uint16
	MsgID       uint16
	Data        []byte
}

// Disconnect is a DISCONNECT message. Duration is present only when HasDuration is set (sleep request).
type Disconnect struct {
	Duration    uint16
	HasDuration bool
}

func (Connect) isMessage()    {}
func (Connack) isMessage()    {}
func (Register) isMessage()   {}
func (Regack) isMessage()     {}
func (Publish) isMessage()    {}
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
	case Register:
		if m.TopicName == "" {
			return nil, fmt.Errorf("%w: empty topic name", ErrMalformed)
		}
		typ = msgRegister
		body = append([]byte{byte(m.TopicID >> 8), byte(m.TopicID), byte(m.MsgID >> 8), byte(m.MsgID)}, m.TopicName...)
	case Regack:
		typ = msgRegack
		body = []byte{byte(m.TopicID >> 8), byte(m.TopicID), byte(m.MsgID >> 8), byte(m.MsgID), m.ReturnCode}
	case Publish:
		if m.QoS > qosMask || m.TopicIDType > topicMask {
			return nil, fmt.Errorf("mqttsn: invalid PUBLISH flags: QoS %d, TopicIDType %d", m.QoS, m.TopicIDType)
		}
		typ = msgPublish
		flags := m.TopicIDType | m.QoS<<qosShift
		if m.DUP {
			flags |= flagDUP
		}
		if m.Retain {
			flags |= flagRetain
		}
		body = []byte{flags, byte(m.TopicID >> 8), byte(m.TopicID), byte(m.MsgID >> 8), byte(m.MsgID)}
		body = append(body, m.Data...)
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
	case msgRegister:
		if len(body) < 5 {
			return nil, ErrMalformed
		}
		return Register{
			TopicID:   uint16(body[0])<<8 | uint16(body[1]),
			MsgID:     uint16(body[2])<<8 | uint16(body[3]),
			TopicName: string(body[4:]),
		}, nil
	case msgRegack:
		if len(body) != 5 {
			return nil, ErrMalformed
		}
		return Regack{
			TopicID:    uint16(body[0])<<8 | uint16(body[1]),
			MsgID:      uint16(body[2])<<8 | uint16(body[3]),
			ReturnCode: body[4],
		}, nil
	case msgPublish:
		if len(body) < 5 {
			return nil, ErrMalformed
		}
		flags := body[0]
		// Data is copied: data aliases the caller's read buffer, which is reused.
		return Publish{
			DUP:         flags&flagDUP != 0,
			QoS:         (flags >> qosShift) & qosMask,
			Retain:      flags&flagRetain != 0,
			TopicIDType: flags & topicMask,
			TopicID:     uint16(body[1])<<8 | uint16(body[2]),
			MsgID:       uint16(body[3])<<8 | uint16(body[4]),
			Data:        append([]byte(nil), body[5:]...),
		}, nil
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
