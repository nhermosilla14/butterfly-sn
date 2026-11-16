package mqtt

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestPacketRemainingLengthRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 127, 128, 16383, 16384, 2097151, 2097152} {
		body := make([]byte, n)
		pkt, err := packet(3, 1, body)
		if err != nil {
			t.Fatalf("packet(%d): %v", n, err)
		}
		p, err := ReadPacket(bytes.NewReader(pkt))
		if err != nil {
			t.Fatalf("ReadPacket(%d): %v", n, err)
		}
		if p.Type != 3 || p.Flags != 1 || len(p.Body) != n {
			t.Fatalf("n=%d: got type %d flags %d len %d", n, p.Type, p.Flags, len(p.Body))
		}
	}
}

func TestReadPacketReadsExactlyOnePacket(t *testing.T) {
	first, _ := packet(typeConnack, 0, []byte{0, 0})
	second, _ := packet(typeDisconnect, 0, nil)
	r := bytes.NewReader(append(first, second...))
	if _, err := ReadPacket(r); err != nil {
		t.Fatal(err)
	}
	p, err := ReadPacket(r)
	if err != nil || p.Type != typeDisconnect || len(p.Body) != 0 {
		t.Fatalf("second packet: %+v, %v", p, err)
	}
}

func TestReadPacketShortInput(t *testing.T) {
	pkt, _ := packet(typeConnack, 0, []byte{0, 0})
	for i := 0; i < len(pkt); i++ {
		if _, err := ReadPacket(bytes.NewReader(pkt[:i])); err == nil {
			t.Fatalf("prefix of %d bytes: no error", i)
		}
	}
}

func TestReadPacketRemainingLengthTooLong(t *testing.T) {
	_, err := ReadPacket(bytes.NewReader([]byte{0x20, 0x80, 0x80, 0x80, 0x80, 0x01}))
	if !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("got %v, want ErrMalformedPacket", err)
	}
}

func TestReadPacketEOF(t *testing.T) {
	if _, err := ReadPacket(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func FuzzReadPacket(f *testing.F) {
	f.Add([]byte{0x20, 0x02, 0x00, 0x00})
	f.Add([]byte{0xE0, 0x00})
	f.Add([]byte{0x30, 0xFF, 0xFF, 0xFF, 0x7F})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadPacket(bytes.NewReader(data))
	})
}
