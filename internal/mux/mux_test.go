package mux

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
	"lukechampine.com/frand"
)

func TestMux(t *testing.T) {
	serverKey := ed25519.NewKeyFromSeed(frand.Bytes(ed25519.SeedSize))
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	serverCh := make(chan error, 1)
	go func() {
		serverCh <- func() error {
			conn, err := l.Accept()
			if err != nil {
				return err
			}
			m, err := Accept(conn, serverKey)
			if err != nil {
				return err
			}
			defer m.Close()
			s, err := m.AcceptStream()
			if err != nil {
				return err
			}
			defer s.Close()
			buf := make([]byte, 13)
			if _, err := io.ReadFull(s, buf); err != nil {
				return err
			}
			if string(buf) != "hello, world!" {
				return errors.New("bad hello")
			}
			return s.Close()
		}()
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	m, err := Dial(conn, serverKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	s := m.DialStream()
	defer s.Close()
	if _, err := s.Write([]byte("hello, world!")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	} else if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	if err := <-serverCh; err != nil && err != ErrPeerClosedStream {
		t.Fatal(err)
	}
}

func TestManyStreams(t *testing.T) {
	serverKey := ed25519.NewKeyFromSeed(frand.Bytes(ed25519.SeedSize))
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	serverCh := make(chan error, 1)
	go func() {
		serverCh <- func() error {
			conn, err := l.Accept()
			if err != nil {
				return err
			}
			m, err := Accept(conn, serverKey)
			if err != nil {
				return err
			}
			defer m.Close()
			for {
				s, err := m.AcceptStream()
				if err != nil {
					return err
				}
				// simple echo handler
				go func() {
					buf := make([]byte, 100)
					n, _ := s.Read(buf)
					s.Write(buf[:n])
					s.Close()
				}()
			}
		}()
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	m, err := Dial(conn, serverKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// spawn 100 streams
	var wg sync.WaitGroup
	errChan := make(chan error, 100)
	for i := 0; i < cap(errChan); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := m.DialStream()
			defer s.Close()
			msg := fmt.Sprintf("hello, %v!", i)
			buf := make([]byte, len(msg))
			if _, err := s.Write([]byte(msg)); err != nil {
				errChan <- err
			} else if _, err := io.ReadFull(s, buf); err != nil {
				errChan <- err
			} else if string(buf) != msg {
				errChan <- err
			} else if err := s.Close(); err != nil {
				errChan <- err
			}
		}(i)
	}
	wg.Wait()
	close(errChan)
	for err := range errChan {
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := m.Close(); err != nil {
		t.Fatal(err)
	} else if err := <-serverCh; err != nil && err != ErrPeerClosedConn {
		t.Fatal(err)
	}
}

func TestDeadline(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	serverKey := ed25519.NewKeyFromSeed(frand.Bytes(ed25519.SeedSize))
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	serverCh := make(chan error, 1)
	go func() {
		serverCh <- func() error {
			conn, err := l.Accept()
			if err != nil {
				return err
			}
			m, err := Accept(conn, serverKey)
			if err != nil {
				return err
			}
			defer m.Close()
			for {
				s, err := m.AcceptStream()
				if err != nil {
					return err
				}
				// wait 100ms before reading/writing
				buf := make([]byte, 100)
				time.Sleep(100 * time.Millisecond)
				if _, err := s.Read(buf); err != nil {
					return err
				}
				time.Sleep(100 * time.Millisecond)
				if _, err := s.Write([]byte("hello, world!")); err != nil {
					return err
				} else if err := s.Close(); err != nil {
					return err
				}
			}
		}()
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	m, err := Dial(conn, serverKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// a Read deadline should not timeout a Write
	s := m.DialStream()
	buf := []byte("hello, world!")
	s.SetReadDeadline(time.Now().Add(time.Millisecond))
	time.Sleep(2 * time.Millisecond)
	_, err = s.Write(buf)
	s.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatal("SetReadDeadline caused Write to fail:", err)
	} else if _, err := io.ReadFull(s, buf); err != nil {
		t.Fatal(err)
	} else if string(buf) != "hello, world!" {
		t.Fatal("bad echo")
	} else if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		timeout bool
		fn      func(*Stream)
	}{
		{false, func(*Stream) {}}, // no deadline
		{false, func(s *Stream) {
			s.SetDeadline(time.Now().Add(time.Hour)) // plenty of time
		}},
		{true, func(s *Stream) {
			s.SetDeadline(time.Now().Add(time.Millisecond)) // too short
		}},
		{true, func(s *Stream) {
			s.SetDeadline(time.Now().Add(time.Millisecond))
			s.SetReadDeadline(time.Time{}) // Write should still fail
		}},
		{true, func(s *Stream) {
			s.SetDeadline(time.Now().Add(time.Millisecond))
			s.SetWriteDeadline(time.Time{}) // Read should still fail
		}},
		{false, func(s *Stream) {
			s.SetDeadline(time.Now())
			s.SetDeadline(time.Time{}) // should overwrite
		}},
		{false, func(s *Stream) {
			s.SetDeadline(time.Now().Add(time.Millisecond))
			s.SetWriteDeadline(time.Time{}) // overwrites Read
			s.SetReadDeadline(time.Time{})  // overwrites Write
		}},
	}
	for i, test := range tests {
		err := func() error {
			s := m.DialStream()
			defer s.Close()
			test.fn(s) // set deadlines

			// need to write a fairly large message; otherwise the packets just
			// get buffered and "succeed" instantly
			if _, err := s.Write(make([]byte, 1<<20)); err != nil {
				return err
			} else if _, err := io.ReadFull(s, buf[:13]); err != nil {
				return err
			} else if string(buf) != "hello, world!" {
				return errors.New("bad echo")
			}
			return s.Close()
		}()
		if isTimeout := errors.Is(err, os.ErrDeadlineExceeded); test.timeout != isTimeout {
			t.Errorf("test %v: expected timeout=%v, got %v", i, test.timeout, err)
		}
	}

	if err := m.Close(); err != nil {
		t.Fatal(err)
	} else if err := <-serverCh; err != nil && err != ErrPeerClosedConn && err != ErrPeerClosedStream {
		t.Fatal(err)
	}
}

func BenchmarkMux(b *testing.B) {
	for _, numStreams := range []int{1, 2, 10, 100, 500, 1000} {
		b.Run(fmt.Sprint(numStreams), func(b *testing.B) {
			serverKey := ed25519.NewKeyFromSeed(frand.Bytes(ed25519.SeedSize))
			l, err := net.Listen("tcp", ":0")
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			serverCh := make(chan error, 1)
			go func() {
				serverCh <- func() error {
					conn, err := l.Accept()
					if err != nil {
						return err
					}
					m, err := Accept(conn, serverKey)
					if err != nil {
						return err
					}
					defer m.Close()
					for {
						s, err := m.AcceptStream()
						if err != nil {
							return err
						}
						go func() {
							io.Copy(io.Discard, s)
							s.Close()
						}()
					}
				}()
			}()
			defer func() {
				if err := <-serverCh; err != nil && err != ErrPeerClosedConn {
					b.Fatal(err)
				}
			}()

			conn, err := net.Dial("tcp", l.Addr().String())
			if err != nil {
				b.Fatal(err)
			}
			m, err := Dial(conn, serverKey.Public().(ed25519.PublicKey))
			if err != nil {
				b.Fatal(err)
			}
			defer m.Close()

			// open each stream in a separate goroutine
			settings, err := defaultConnSettings(conn)
			if err != nil {
				b.Fatal(err)
			}
			bufSize := settings.maxPayloadSize()
			buf := make([]byte, bufSize)
			b.ResetTimer()
			b.SetBytes(int64(bufSize * numStreams))
			b.ReportAllocs()
			start := time.Now()
			var wg sync.WaitGroup
			wg.Add(numStreams)
			for j := 0; j < numStreams; j++ {
				go func() {
					defer wg.Done()
					s := m.DialStream()
					defer s.Close()
					for i := 0; i < b.N; i++ {
						if _, err := s.Write(buf); err != nil {
							panic(err)
						}
					}
				}()
			}
			wg.Wait()
			b.ReportMetric(float64(b.N*numStreams)/time.Since(start).Seconds(), "conns/sec")
		})
	}
}

func BenchmarkConn(b *testing.B) {
	// benchmark throughput of raw TCP conn (plus encryption overhead to make it fair)
	encryptionKey := frand.Bytes(32)
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		b.Fatal(err)
	}
	serverCh := make(chan error, 1)
	go func() {
		serverCh <- func() error {
			conn, err := l.Accept()
			if err != nil {
				return err
			}
			defer conn.Close()
			settings, err := defaultConnSettings(conn)
			if err != nil {
				return err
			}
			aead, _ := chacha20poly1305.New(encryptionKey)
			buf := make([]byte, settings.maxFrameSize())
			for {
				_, err := io.ReadFull(conn, buf)
				if err != nil {
					return err
				}
				if _, err := decryptInPlace(buf, aead); err != nil {
					return err
				}
			}
		}()
	}()
	defer func() {
		if err := <-serverCh; err != nil && err != io.EOF {
			b.Fatal(err)
		}
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	settings, err := defaultConnSettings(conn)
	if err != nil {
		b.Fatal(err)
	}

	aead, _ := chacha20poly1305.New(encryptionKey)
	buf := make([]byte, settings.maxFrameSize())
	b.ResetTimer()
	b.SetBytes(int64(settings.maxPayloadSize()))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encryptInPlace(buf, aead)
		if _, err := conn.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
}
