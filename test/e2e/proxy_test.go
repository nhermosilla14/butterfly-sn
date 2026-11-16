package e2e

import (
	"net"
	"sync"
	"testing"
)

// tcpProxy listens on 127.0.0.1 and forwards each accepted connection to target. Tests cut the live
// connections or write raw bytes towards the gateway, without touching the broker itself.
type tcpProxy struct {
	t      *testing.T
	ln     net.Listener
	target string
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
	pairs  []*proxyPair
}

// proxyPair is one gateway connection and its upstream connection. wmu serialises writes to gw, so
// injected bytes never interleave with bytes forwarded from the broker.
type proxyPair struct {
	gw, up    net.Conn
	wmu       sync.Mutex
	closeOnce sync.Once
}

func startProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{t: t, ln: ln, target: target}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(p.close)
	return p
}

// addr is the address the gateway must use as its broker.
func (p *tcpProxy) addr() string {
	return p.ln.Addr().String()
}

func (p *tcpProxy) accept() {
	defer p.wg.Done()
	for {
		gw, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.DialTimeout("tcp", p.target, waitShort)
		if err != nil {
			_ = gw.Close()
			continue
		}
		pair := &proxyPair{gw: gw, up: up}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = gw.Close()
			_ = up.Close()
			return
		}
		p.pairs = append(p.pairs, pair)
		p.wg.Add(2)
		p.mu.Unlock()
		go p.pump(pair, pair.up, pair.gw, true)
		go p.pump(pair, pair.gw, pair.up, false)
	}
}

// pump copies src to dst until a read or write fails, then closes the whole pair so the other direction ends too.
// Writes to the gateway take the pair's write lock, so injected bytes stay whole.
func (p *tcpProxy) pump(pair *proxyPair, src, dst net.Conn, toGateway bool) {
	defer p.wg.Done()
	defer pair.close()
	buf := make([]byte, 4096)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if toGateway {
				pair.wmu.Lock()
			}
			_, werr := dst.Write(buf[:n])
			if toGateway {
				pair.wmu.Unlock()
			}
			if werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (pr *proxyPair) close() {
	pr.closeOnce.Do(func() {
		_ = pr.gw.Close()
		_ = pr.up.Close()
	})
}

// dropAll closes every live connection in both directions. The gateway sees its broker connection end.
func (p *tcpProxy) dropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pair := range p.pairs {
		pair.close()
	}
}

// injectToGateway writes raw bytes on the newest gateway-facing connection, as if the broker had sent them.
func (p *tcpProxy) injectToGateway(b []byte) {
	p.t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pairs) == 0 {
		p.t.Fatal("proxy: no gateway connection to inject into")
	}
	pair := p.pairs[len(p.pairs)-1]
	pair.wmu.Lock()
	defer pair.wmu.Unlock()
	if _, err := pair.gw.Write(b); err != nil {
		p.t.Fatalf("proxy: inject: %v", err)
	}
}

// close stops the listener and every connection, then waits for the goroutines to end.
func (p *tcpProxy) close() {
	p.mu.Lock()
	p.closed = true
	pairs := p.pairs
	p.mu.Unlock()
	_ = p.ln.Close()
	for _, pair := range pairs {
		pair.close()
	}
	p.wg.Wait()
}
