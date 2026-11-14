// Package mqttsn encodes and decodes MQTT-SN v1.2 messages. It does no I/O.
package mqttsn

import (
	"errors"
	"fmt"
)

const (
	msgAdvertise    = 0x00
	msgSearchgw     = 0x01
	msgGwinfo       = 0x02
	msgConnect      = 0x04
	msgConnack      = 0x05
	msgRegister     = 0x0A
	msgRegack       = 0x0B
	msgPublish      = 0x0C
	msgPuback       = 0x0D
	msgSubscribe    = 0x12
	msgSuback       = 0x13
	msgUnsubscribe  = 0x14
	msgUnsuback     = 0x15
	msgWillTopicReq = 0x06
	msgWillTopic    = 0x07
	msgWillMsgReq   = 0x08
	msgWillMsg      = 0x09
	msgPingreq      = 0x16
	msgPingresp     = 0x17
	msgDisconnect   = 0x18

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

// Message is one decoded MQTT-SN message: Advertise, Searchgw, Gwinfo, Connect, Connack, Register, Regack,
// Publish, Puback, Subscribe, Suback, Unsubscribe, Unsuback, Pingreq, Pingresp, Disconnect, WillTopicReq,
// WillTopic, WillMsgReq or WillMsg.
type Message interface {
	isMessage()
}

// Advertise is an ADVERTISE message: the gateway id and the interval, in seconds, until the next one.
type Advertise struct {
	GwID     byte
	Duration uint16
}

// Searchgw is a SEARCHGW message from a client looking for a gateway. Radius is only present when HasRadius
// is set.
type Searchgw struct {
	Radius    byte
	HasRadius bool
}

// Gwinfo is a GWINFO message answering a Searchgw. Address is optional and nil when absent.
type Gwinfo struct {
	GwID    byte
	Address []byte
}

// Connect is a CONNECT message. Will tells the gateway that the will handshake must run before the broker.
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

// Puback is a PUBACK message answering a QoS 1 Publish with the same TopicID and MsgID.
type Puback struct {
	TopicID    uint16
	MsgID      uint16
	ReturnCode byte
}

// Subscribe is a SUBSCRIBE message. TopicName is a topic name, a topic filter or a 2-character short name,
// as TopicIDType says; it is never empty. QoS is the requested QoS. MsgID is echoed by Suback.
type Subscribe struct {
	QoS         byte
	TopicIDType byte
	MsgID       uint16
	TopicName   string
}

// Suback is a SUBACK message answering a Subscribe with the same MsgID. TopicID is only meaningful when
// ReturnCode is ReturnAccepted.
type Suback struct {
	QoS        byte
	TopicID    uint16
	MsgID      uint16
	ReturnCode byte
}

// Unsubscribe is an UNSUBSCRIBE message. TopicName is never empty, as for Subscribe.
type Unsubscribe struct {
	TopicIDType byte
	MsgID       uint16
	TopicName   string
}

// Unsuback is an UNSUBACK message answering an Unsubscribe with the same MsgID.
type Unsuback struct {
	MsgID uint16
}

// Pingreq is a PINGREQ message. ClientID is optional: active clients normally send none.
type Pingreq struct {
	ClientID string
}

// Pingresp is a PINGRESP message, which has no body.
type Pingresp struct{}

// Disconnect is a DISCONNECT message. Duration is present only when HasDuration is set (sleep request).
type Disconnect struct {
	Duration    uint16
	HasDuration bool
}

// WillTopicReq asks the client for its will topic. It has no body.
type WillTopicReq struct{}

// WillTopic carries the will topic, QoS and retain flag. An empty Topic means the client wants no will: the
// message is then exactly two bytes, and QoS and Retain must be zero. DUP, Will, CleanSession and TopicIDType
// are not used and are not reported.
type WillTopic struct {
	QoS    byte
	Retain bool
	Topic  string
}

// WillMsgReq asks the client for its will message. It has no body.
type WillMsgReq struct{}

// WillMsg carries the will payload. Payload may be empty.
type WillMsg struct {
	Payload []byte
}

func (Advertise) isMessage()    {}
func (Searchgw) isMessage()     {}
func (Gwinfo) isMessage()       {}
func (Connect) isMessage()      {}
func (Connack) isMessage()      {}
func (Register) isMessage()     {}
func (Regack) isMessage()       {}
func (Publish) isMessage()      {}
func (Puback) isMessage()       {}
func (Subscribe) isMessage()    {}
func (Suback) isMessage()       {}
func (Unsubscribe) isMessage()  {}
func (Unsuback) isMessage()     {}
func (Pingreq) isMessage()      {}
func (Pingresp) isMessage()     {}
func (Disconnect) isMessage()   {}
func (WillTopicReq) isMessage() {}
func (WillTopic) isMessage()    {}
func (WillMsgReq) isMessage()   {}
func (WillMsg) isMessage()      {}

// Encode serialises m with its length header.
func Encode(m Message) ([]byte, error) {
	var typ byte
	var body []byte
	switch m := m.(type) {
	case Advertise:
		typ = msgAdvertise
		body = []byte{m.GwID, byte(m.Duration >> 8), byte(m.Duration)}
	case Searchgw:
		typ = msgSearchgw
		if m.HasRadius {
			body = []byte{m.Radius}
		}
	case Gwinfo:
		typ = msgGwinfo
		body = append([]byte{m.GwID}, m.Address...)
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
	case Puback:
		typ = msgPuback
		body = []byte{byte(m.TopicID >> 8), byte(m.TopicID), byte(m.MsgID >> 8), byte(m.MsgID), m.ReturnCode}
	case Subscribe:
		if m.TopicName == "" {
			return nil, fmt.Errorf("%w: empty topic name", ErrMalformed)
		}
		if m.QoS > qosMask || m.TopicIDType > topicMask {
			return nil, fmt.Errorf("mqttsn: invalid SUBSCRIBE flags: QoS %d, TopicIDType %d", m.QoS, m.TopicIDType)
		}
		typ = msgSubscribe
		body = append([]byte{m.TopicIDType | m.QoS<<qosShift, byte(m.MsgID >> 8), byte(m.MsgID)}, m.TopicName...)
	case Suback:
		if m.QoS > qosMask {
			return nil, fmt.Errorf("mqttsn: invalid SUBACK QoS %d", m.QoS)
		}
		typ = msgSuback
		body = []byte{m.QoS << qosShift, byte(m.TopicID >> 8), byte(m.TopicID), byte(m.MsgID >> 8), byte(m.MsgID), m.ReturnCode}
	case Unsubscribe:
		if m.TopicName == "" {
			return nil, fmt.Errorf("%w: empty topic name", ErrMalformed)
		}
		if m.TopicIDType > topicMask {
			return nil, fmt.Errorf("mqttsn: invalid UNSUBSCRIBE TopicIDType %d", m.TopicIDType)
		}
		typ = msgUnsubscribe
		body = append([]byte{m.TopicIDType, byte(m.MsgID >> 8), byte(m.MsgID)}, m.TopicName...)
	case Unsuback:
		typ = msgUnsuback
		body = []byte{byte(m.MsgID >> 8), byte(m.MsgID)}
	case Pingreq:
		typ = msgPingreq
		body = []byte(m.ClientID)
	case Pingresp:
		typ = msgPingresp
	case Disconnect:
		typ = msgDisconnect
		if m.HasDuration {
			body = []byte{byte(m.Duration >> 8), byte(m.Duration)}
		}
	case WillTopicReq:
		typ = msgWillTopicReq
	case WillTopic:
		typ = msgWillTopic
		if m.Topic != "" {
			if m.QoS > qosMask {
				return nil, fmt.Errorf("mqttsn: invalid WILLTOPIC QoS %d", m.QoS)
			}
			flags := m.QoS << qosShift
			if m.Retain {
				flags |= flagRetain
			}
			body = append([]byte{flags}, m.Topic...)
		} else if m.QoS != 0 || m.Retain {
			return nil, fmt.Errorf("%w: empty will topic with QoS or retain", ErrMalformed)
		}
	case WillMsgReq:
		typ = msgWillMsgReq
	case WillMsg:
		typ = msgWillMsg
		body = m.Payload
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
	case msgAdvertise:
		if len(body) != 3 {
			return nil, ErrMalformed
		}
		return Advertise{GwID: body[0], Duration: uint16(body[1])<<8 | uint16(body[2])}, nil
	case msgSearchgw:
		switch len(body) {
		case 0:
			return Searchgw{}, nil
		case 1:
			return Searchgw{Radius: body[0], HasRadius: true}, nil
		}
		return nil, ErrMalformed
	case msgGwinfo:
		if len(body) < 1 {
			return nil, ErrMalformed
		}
		// Address is copied: data aliases the caller's read buffer, which is reused.
		return Gwinfo{GwID: body[0], Address: append([]byte(nil), body[1:]...)}, nil
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
	case msgPuback:
		if len(body) != 5 {
			return nil, ErrMalformed
		}
		return Puback{
			TopicID:    uint16(body[0])<<8 | uint16(body[1]),
			MsgID:      uint16(body[2])<<8 | uint16(body[3]),
			ReturnCode: body[4],
		}, nil
	case msgSubscribe:
		if len(body) < 4 {
			return nil, ErrMalformed
		}
		return Subscribe{
			QoS:         (body[0] >> qosShift) & qosMask,
			TopicIDType: body[0] & topicMask,
			MsgID:       uint16(body[1])<<8 | uint16(body[2]),
			TopicName:   string(body[3:]),
		}, nil
	case msgSuback:
		if len(body) != 6 {
			return nil, ErrMalformed
		}
		return Suback{
			QoS:        (body[0] >> qosShift) & qosMask,
			TopicID:    uint16(body[1])<<8 | uint16(body[2]),
			MsgID:      uint16(body[3])<<8 | uint16(body[4]),
			ReturnCode: body[5],
		}, nil
	case msgUnsubscribe:
		if len(body) < 4 {
			return nil, ErrMalformed
		}
		return Unsubscribe{
			TopicIDType: body[0] & topicMask,
			MsgID:       uint16(body[1])<<8 | uint16(body[2]),
			TopicName:   string(body[3:]),
		}, nil
	case msgUnsuback:
		if len(body) != 2 {
			return nil, ErrMalformed
		}
		return Unsuback{MsgID: uint16(body[0])<<8 | uint16(body[1])}, nil
	case msgPingreq:
		return Pingreq{ClientID: string(body)}, nil
	case msgPingresp:
		if len(body) != 0 {
			return nil, ErrMalformed
		}
		return Pingresp{}, nil
	case msgDisconnect:
		switch len(body) {
		case 0:
			return Disconnect{}, nil
		case 2:
			return Disconnect{Duration: uint16(body[0])<<8 | uint16(body[1]), HasDuration: true}, nil
		}
		return nil, ErrMalformed
	case msgWillTopicReq:
		if len(body) != 0 {
			return nil, ErrMalformed
		}
		return WillTopicReq{}, nil
	case msgWillTopic:
		switch len(body) {
		case 0:
			return WillTopic{}, nil
		case 1:
			return nil, ErrMalformed // flags without a topic name
		}
		return WillTopic{
			QoS:    (body[0] >> qosShift) & qosMask,
			Retain: body[0]&flagRetain != 0,
			Topic:  string(body[1:]),
		}, nil
	case msgWillMsgReq:
		if len(body) != 0 {
			return nil, ErrMalformed
		}
		return WillMsgReq{}, nil
	case msgWillMsg:
		// Payload is copied: data aliases the caller's read buffer, which is reused.
		return WillMsg{Payload: append([]byte(nil), body...)}, nil
	}
	return nil, ErrUnsupported
}
