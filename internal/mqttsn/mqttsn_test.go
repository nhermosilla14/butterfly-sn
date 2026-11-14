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
	for _, m := range []Message{Connect{ClientID: "a"}, Connack{}, Disconnect{Duration: 1, HasDuration: true}} {
		b, _ := Encode(m)
		f.Add(b)
	}
	f.Add([]byte{0x01, 0xFF, 0xFF, 0x04})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Decode(data)
	})
}
