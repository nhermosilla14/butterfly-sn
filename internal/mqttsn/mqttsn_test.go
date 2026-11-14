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

func TestEncodeInvalid(t *testing.T) {
	cases := map[string]Message{
		"register empty name": Register{TopicID: 0, MsgID: 1},
		"publish qos 4":       Publish{QoS: 4},
		"publish topic type":  Publish{TopicIDType: 4},
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
	}
	for name, b := range cases {
		if _, err := Decode(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: got %v, want ErrMalformed", name, err)
		}
	}
}

func TestDecodeUnsupported(t *testing.T) {
	// PINGREQ (0x16) is valid MQTT-SN but not implemented yet.
	if _, err := Decode([]byte{0x02, 0x16}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
}

func FuzzDecode(f *testing.F) {
	for _, m := range []Message{
		Connect{ClientID: "a"}, Connack{}, Disconnect{Duration: 1, HasDuration: true},
		Register{TopicID: 0, MsgID: 1, TopicName: "a/b"}, Regack{TopicID: 1, MsgID: 1},
		Publish{QoS: 1, TopicID: 1, MsgID: 2, Data: []byte("x")}, Puback{TopicID: 1, MsgID: 2},
	} {
		b, _ := Encode(m)
		f.Add(b)
	}
	f.Add([]byte{0x01, 0xFF, 0xFF, 0x04})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Decode(data)
	})
}
