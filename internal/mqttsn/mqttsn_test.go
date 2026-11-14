package mqttsn

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	cases := []Message{
		Connect{CleanSession: true, Duration: 60, ClientID: "sensor-1"},
		Connect{CleanSession: false, Will: true, Duration: 0, ClientID: "a"},
		Connack{ReturnCode: ReturnAccepted},
		Connack{ReturnCode: ReturnCongestion},
		Disconnect{},
		Disconnect{Duration: 300, HasDuration: true},
		Register{TopicName: "sensors/temp"},
		Register{TopicID: 0, MsgID: 0xBEEF, TopicName: "a"},
		Regack{TopicID: 0xFFFE, MsgID: 7, ReturnCode: ReturnAccepted},
		Regack{TopicID: 0, MsgID: 0xFFFF, ReturnCode: ReturnCongestion},
		Publish{QoS: 0, TopicIDType: TopicIDNormal, TopicID: 1, Data: []byte("hi")},
		Publish{DUP: true, QoS: 2, Retain: true, TopicIDType: TopicIDShort, TopicID: 0x6162, MsgID: 9, Data: []byte{0}},
		Publish{QoS: 3, TopicIDType: TopicIDPredefined, TopicID: 3, MsgID: 0},
		Publish{QoS: 0, TopicID: 5, Data: allBytes()},
		Publish{QoS: 0, TopicID: 5},
		Publish{QoS: 1, TopicID: 0xFFFE, MsgID: 0xBEEF, Data: []byte("q1")},
		Puback{TopicID: 0xFFFE, MsgID: 0xBEEF, ReturnCode: ReturnAccepted},
		Puback{TopicID: 1, MsgID: 1, ReturnCode: ReturnInvalidTopicID},
		Subscribe{QoS: 1, TopicIDType: TopicIDNormal, MsgID: 0xBEEF, TopicName: "a/#"},
		Subscribe{QoS: 0, TopicIDType: TopicIDShort, MsgID: 1, TopicName: "ab"},
		Suback{QoS: 1, TopicID: 0xFFFE, MsgID: 0xBEEF, ReturnCode: ReturnAccepted},
		Suback{QoS: 0, TopicID: 0, MsgID: 2, ReturnCode: ReturnCongestion},
		Unsubscribe{TopicIDType: TopicIDNormal, MsgID: 3, TopicName: "a/+"},
		Unsubscribe{TopicIDType: TopicIDShort, MsgID: 0xFFFF, TopicName: "xy"},
		Unsuback{MsgID: 0x0102},
		Pingreq{},
		Pingreq{ClientID: "sensor-1"},
		Pingresp{},
		WillTopicReq{},
		WillTopic{QoS: 1, Retain: true, Topic: "dev/status"},
		WillTopic{QoS: 0, Topic: "a"},
		WillTopic{},
		WillMsgReq{},
		WillMsg{Payload: []byte("offline")},
		WillMsg{Payload: allBytes()},
		WillMsg{},
	}
	for _, m := range cases {
		b, err := Encode(m)
		if err != nil {
			t.Fatalf("Encode(%+v): %v", m, err)
		}
		got, err := Decode(b)
		if err != nil {
			t.Fatalf("Decode(%v): %v", b, err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip: got %+v, want %+v", got, m)
		}
	}
}

func allBytes() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func TestPublishPayloadBinaryAndEmpty(t *testing.T) {
	payload := allBytes()
	b, err := Encode(Publish{TopicID: 2, Data: payload})
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(b)
	if err != nil || !bytes.Equal(got.(Publish).Data, payload) {
		t.Fatalf("binary payload not preserved: %v", err)
	}
	b, _ = Encode(Publish{TopicID: 2})
	if !bytes.Equal(b, []byte{7, 0x0C, 0x00, 0x00, 0x02, 0x00, 0x00}) {
		t.Fatalf("empty payload encoding: % x", b)
	}
}

func TestPublishDecodeCopiesData(t *testing.T) {
	b, _ := Encode(Publish{TopicID: 1, Data: []byte("abc")})
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	for i := range b {
		b[i] = 0 // the caller reuses its read buffer
	}
	if string(got.(Publish).Data) != "abc" {
		t.Fatalf("decoded data aliases input: %q", got.(Publish).Data)
	}
}

func TestPublishFlagsEncoding(t *testing.T) {
	b, err := Encode(Publish{DUP: true, QoS: 2, Retain: true, TopicIDType: TopicIDShort, TopicID: 0x0102, MsgID: 0x0304})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{7, 0x0C, 0x80 | 2<<5 | 0x10 | 0x02, 0x01, 0x02, 0x03, 0x04}
	if !bytes.Equal(b, want) {
		t.Fatalf("got % x, want % x", b, want)
	}
}

func TestPubackEncodeBytes(t *testing.T) {
	b, _ := Encode(Puback{TopicID: 0x0102, MsgID: 0x0304, ReturnCode: ReturnAccepted})
	if !bytes.Equal(b, []byte{7, 0x0D, 0x01, 0x02, 0x03, 0x04, 0}) {
		t.Fatalf("PUBACK got % x", b)
	}
}

func TestRegisterAndRegackEncodeBytes(t *testing.T) {
	b, _ := Encode(Register{MsgID: 1, TopicName: "t"})
	if !bytes.Equal(b, []byte{7, 0x0A, 0, 0, 0, 1, 't'}) {
		t.Fatalf("REGISTER got % x", b)
	}
	b, _ = Encode(Regack{TopicID: 3, MsgID: 1, ReturnCode: ReturnAccepted})
	if !bytes.Equal(b, []byte{7, 0x0B, 0, 3, 0, 1, 0}) {
		t.Fatalf("REGACK got % x", b)
	}
}

func TestSubscribeUnsubscribeEncodeBytes(t *testing.T) {
	b, _ := Encode(Subscribe{QoS: 1, MsgID: 0x0102, TopicName: "t"})
	if !bytes.Equal(b, []byte{6, 0x12, 0x20, 0x01, 0x02, 't'}) {
		t.Fatalf("SUBSCRIBE got % x", b)
	}
	b, _ = Encode(Subscribe{TopicIDType: TopicIDShort, QoS: 2, MsgID: 3, TopicName: "ab"})
	if !bytes.Equal(b, []byte{7, 0x12, 0x40 | 0x02, 0, 3, 'a', 'b'}) {
		t.Fatalf("SUBSCRIBE short got % x", b)
	}
	b, _ = Encode(Suback{QoS: 1, TopicID: 0x0102, MsgID: 0x0304, ReturnCode: ReturnCongestion})
	if !bytes.Equal(b, []byte{8, 0x13, 0x20, 0x01, 0x02, 0x03, 0x04, 0x01}) {
		t.Fatalf("SUBACK got % x", b)
	}
	b, _ = Encode(Unsubscribe{MsgID: 5, TopicName: "a/#"})
	if !bytes.Equal(b, []byte{8, 0x14, 0x00, 0x00, 0x05, 'a', '/', '#'}) {
		t.Fatalf("UNSUBSCRIBE got % x", b)
	}
	b, _ = Encode(Unsubscribe{TopicIDType: TopicIDShort, MsgID: 6, TopicName: "ab"})
	if !bytes.Equal(b, []byte{7, 0x14, 0x02, 0x00, 0x06, 'a', 'b'}) {
		t.Fatalf("UNSUBSCRIBE short got % x", b)
	}
	b, _ = Encode(Unsuback{MsgID: 0x0102})
	if !bytes.Equal(b, []byte{4, 0x15, 0x01, 0x02}) {
		t.Fatalf("UNSUBACK got % x", b)
	}
}

func TestSubscribeDecodeMalformed(t *testing.T) {
	cases := map[string][]byte{
		"subscribe no name":   {5, 0x12, 0x00, 0x00, 0x01},
		"subscribe short":     {4, 0x12, 0x00, 0x01},
		"unsubscribe no name": {5, 0x14, 0x00, 0x00, 0x01},
		"unsubscribe short":   {4, 0x14, 0x00, 0x01},
		"suback short":        {7, 0x13, 0x00, 0x00, 0x01, 0x00, 0x01},
		"suback long":         {9, 0x13, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00},
		"unsuback short":      {3, 0x15, 0x01},
		"unsuback long":       {5, 0x15, 0x00, 0x01, 0x00},
	}
	for name, b := range cases {
		if _, err := Decode(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: got %v, want ErrMalformed", name, err)
		}
	}
}

func TestSubscribeUnsubscribeDecodeBytes(t *testing.T) {
	m, err := Decode([]byte{6, 0x12, 0x20, 0x01, 0x02, 't'})
	want := Subscribe{QoS: 1, MsgID: 0x0102, TopicName: "t"}
	if err != nil || !reflect.DeepEqual(m, want) {
		t.Fatalf("got %+v, %v; want %+v", m, err, want)
	}
	m, err = Decode([]byte{8, 0x13, 0x20, 0x00, 0x09, 0x00, 0x03, 0x80})
	wantAck := Suback{QoS: 1, TopicID: 9, MsgID: 3, ReturnCode: 0x80}
	if err != nil || !reflect.DeepEqual(m, wantAck) {
		t.Fatalf("SUBACK got %+v, %v; want %+v", m, err, wantAck)
	}
	m, err = Decode([]byte{7, 0x12, 0x22, 0x00, 0x04, 'a', 'b'})
	wantShort := Subscribe{QoS: 1, TopicIDType: TopicIDShort, MsgID: 4, TopicName: "ab"}
	if err != nil || !reflect.DeepEqual(m, wantShort) {
		t.Fatalf("SUBSCRIBE short got %+v, %v; want %+v", m, err, wantShort)
	}
	m, err = Decode([]byte{7, 0x14, 0x02, 0x00, 0x05, 'a', 'b'})
	wantUnsub := Unsubscribe{TopicIDType: TopicIDShort, MsgID: 5, TopicName: "ab"}
	if err != nil || !reflect.DeepEqual(m, wantUnsub) {
		t.Fatalf("UNSUBSCRIBE short got %+v, %v; want %+v", m, err, wantUnsub)
	}
	m, err = Decode([]byte{8, 0x0C, 0x02, 0x61, 0x62, 0x00, 0x01, 'x'})
	wantPub := Publish{TopicIDType: TopicIDShort, TopicID: 0x6162, MsgID: 1, Data: []byte("x")}
	if err != nil || !reflect.DeepEqual(m, wantPub) {
		t.Fatalf("PUBLISH short got %+v, %v; want %+v", m, err, wantPub)
	}
}

func TestEncodeInvalid(t *testing.T) {
	cases := map[string]Message{
		"subscribe empty name":     Subscribe{MsgID: 1},
		"subscribe qos 4":          Subscribe{QoS: 4, TopicName: "t"},
		"subscribe topic type":     Subscribe{TopicIDType: 4, TopicName: "t"},
		"suback qos 4":             Suback{QoS: 4},
		"unsubscribe empty name":   Unsubscribe{MsgID: 1},
		"unsubscribe topic type":   Unsubscribe{TopicIDType: 4, TopicName: "t"},
		"register empty name":      Register{TopicID: 0, MsgID: 1},
		"publish qos 4":            Publish{QoS: 4},
		"publish topic type":       Publish{TopicIDType: 4},
		"willtopic qos 4":          WillTopic{QoS: 4, Topic: "t"},
		"willtopic empty with qos": WillTopic{QoS: 1},
		"willtopic empty retain":   WillTopic{Retain: true},
	}
	for name, m := range cases {
		if _, err := Encode(m); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestEncodeBytes(t *testing.T) {
	b, _ := Encode(Connect{CleanSession: true, Duration: 0x0102, ClientID: "ab"})
	want := []byte{8, 0x04, 0x04, 0x01, 0x01, 0x02, 'a', 'b'}
	if !bytes.Equal(b, want) {
		t.Fatalf("got %v, want %v", b, want)
	}
	b, _ = Encode(Disconnect{})
	if !bytes.Equal(b, []byte{2, 0x18}) {
		t.Fatalf("DISCONNECT got %v", b)
	}
}

func TestLongLengthHeader(t *testing.T) {
	id := strings.Repeat("x", 300)
	b, err := Encode(Connect{ClientID: id})
	if err != nil {
		t.Fatal(err)
	}
	if b[0] != 0x01 || len(b) != 3+1+4+len(id) {
		t.Fatalf("header: % x (len %d)", b[:4], len(b))
	}
	got, err := Decode(b)
	if err != nil || got.(Connect).ClientID != id {
		t.Fatalf("decode long: %v", err)
	}
}

func TestEncodeTooLong(t *testing.T) {
	if _, err := Encode(Connect{ClientID: strings.Repeat("x", 70000)}); err == nil {
		t.Fatal("no error for oversized message")
	}
}

func TestDecodeErrors(t *testing.T) {
	cases := map[string][]byte{
		"empty":                        {},
		"one byte":                     {0x02},
		"zero length":                  {0x00, 0x05},
		"length longer than datagram":  {0x05, 0x05, 0x00},
		"length shorter than datagram": {0x02, 0x18, 0x00},
		"long header truncated":        {0x01, 0x00},
		"long length mismatch":         {0x01, 0x00, 0x09, 0x05, 0x00},
		"connack no code":              {0x02, 0x05},
		"connack two bytes":            {0x04, 0x05, 0x00, 0x00},
		"connect short":                {0x05, 0x04, 0x04, 0x01, 0x00},
		"connect bad protocol":         {0x08, 0x04, 0x04, 0x02, 0x00, 0x00, 'a', 'b'},
		"disconnect 1 byte":            {0x03, 0x18, 0x01},
		"disconnect 3 bytes":           {0x05, 0x18, 0, 0, 0},
		"register no name":             {0x06, 0x0A, 0, 0, 0, 1},
		"register short":               {0x05, 0x0A, 0, 0, 0},
		"regack short":                 {0x06, 0x0B, 0, 1, 0, 1},
		"regack long":                  {0x08, 0x0B, 0, 1, 0, 1, 0, 0},
		"publish short":                {0x06, 0x0C, 0, 1, 0, 1},
		"publish no body":              {0x02, 0x0C},
		"puback short":                 {0x06, 0x0D, 0, 1, 0, 1},
		"puback long":                  {0x08, 0x0D, 0, 1, 0, 1, 0, 0},
		"puback no body":               {0x02, 0x0D},
		"pingresp with body":           {0x03, 0x17, 0x00},
		"pingresp long body":           {0x04, 0x17, 'a', 'b'},
		"willtopicreq with body":       {0x03, 0x06, 0x00},
		"willtopic flags only":         {0x03, 0x07, 0x20},
		"willmsgreq with body":         {0x03, 0x08, 0x00},
	}
	for name, b := range cases {
		if _, err := Decode(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: got %v, want ErrMalformed", name, err)
		}
	}
}

func TestPingEncodeBytes(t *testing.T) {
	b, _ := Encode(Pingreq{})
	if !bytes.Equal(b, []byte{2, 0x16}) {
		t.Fatalf("PINGREQ got % x", b)
	}
	b, _ = Encode(Pingreq{ClientID: "ab"})
	if !bytes.Equal(b, []byte{4, 0x16, 'a', 'b'}) {
		t.Fatalf("PINGREQ with client id got % x", b)
	}
	b, _ = Encode(Pingresp{})
	if !bytes.Equal(b, []byte{2, 0x17}) {
		t.Fatalf("PINGRESP got % x", b)
	}
}

func TestPingDecodeEmptyClientID(t *testing.T) {
	got, err := Decode([]byte{2, 0x16})
	if err != nil || got != (Pingreq{}) {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestWillEncodeBytes(t *testing.T) {
	b, _ := Encode(WillTopicReq{})
	if !bytes.Equal(b, []byte{2, 0x06}) {
		t.Fatalf("WILLTOPICREQ got % x", b)
	}
	b, _ = Encode(WillMsgReq{})
	if !bytes.Equal(b, []byte{2, 0x08}) {
		t.Fatalf("WILLMSGREQ got % x", b)
	}
	b, _ = Encode(WillTopic{})
	if !bytes.Equal(b, []byte{2, 0x07}) {
		t.Fatalf("empty WILLTOPIC got % x", b)
	}
	b, _ = Encode(WillTopic{QoS: 1, Retain: true, Topic: "ab"})
	if !bytes.Equal(b, []byte{5, 0x07, 0x20 | 0x10, 'a', 'b'}) {
		t.Fatalf("WILLTOPIC got % x", b)
	}
	b, _ = Encode(WillMsg{Payload: []byte("xy")})
	if !bytes.Equal(b, []byte{4, 0x09, 'x', 'y'}) {
		t.Fatalf("WILLMSG got % x", b)
	}
}

func TestWillTopicDecodeIgnoresUnusedFlags(t *testing.T) {
	// DUP, Will, CleanSession and TopicIdType bits are set; only QoS and Retain are read.
	got, err := Decode([]byte{6, 0x07, 0x80 | 0x08 | 0x04 | 0x02 | 0x01, 'a', 'b', 'c'})
	if err != nil || got != (WillTopic{Topic: "abc"}) {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestDecodeUnsupported(t *testing.T) {
	// ADVERTISE (0x00) is valid MQTT-SN but not implemented yet.
	if _, err := Decode([]byte{0x02, 0x00}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
}

func FuzzDecode(f *testing.F) {
	for _, m := range []Message{
		Connect{ClientID: "a"}, Connack{}, Disconnect{Duration: 1, HasDuration: true},
		Register{TopicID: 0, MsgID: 1, TopicName: "a/b"}, Regack{TopicID: 1, MsgID: 1},
		Publish{QoS: 1, TopicID: 1, MsgID: 2, Data: []byte("x")}, Puback{TopicID: 1, MsgID: 2},
		Subscribe{MsgID: 1, TopicName: "a/#"}, Suback{MsgID: 1}, Unsubscribe{MsgID: 1, TopicName: "a"}, Unsuback{MsgID: 1},
		Pingreq{}, Pingreq{ClientID: "a"}, Pingresp{},
		WillTopicReq{}, WillTopic{QoS: 1, Retain: true, Topic: "a"}, WillMsgReq{}, WillMsg{Payload: []byte("x")},
	} {
		b, _ := Encode(m)
		f.Add(b)
	}
	f.Add([]byte{0x01, 0xFF, 0xFF, 0x04})
	f.Add([]byte{6, 0x12, 0x20, 0x00, 0x01, 'a'})
	f.Add([]byte{8, 0x13, 0x00, 0x00, 0x01, 0x00, 0x02, 0x80})
	f.Add([]byte{5, 0x14, 0x00, 0x00, 0x01, 'a'})
	f.Add([]byte{7, 0x12, 0x22, 0x00, 0x01, 'a', 'b'})
	f.Add([]byte{7, 0x14, 0x02, 0x00, 0x01, 'a', 'b'})
	f.Add([]byte{8, 0x0C, 0x02, 0x00, 0x00, 0x00, 0x01, 'x'})
	f.Add([]byte{4, 0x15, 0x00, 0x01})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Decode(data)
	})
}
