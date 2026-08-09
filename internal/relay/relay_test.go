package relay

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	framestream "github.com/farsightsec/golang-framestream"
)

// fakeProducer dials the relay's unix socket, performs the FSTRM handshake as
// a writer, and streams n frames into the connection. It retries the dial
// until the relay is listening.
func fakeProducer(sock string, n int, done chan error) {
	var conn net.Conn
	var err error
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err = net.DialTimeout("unix", sock, time.Second)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			done <- fmt.Errorf("producer dial: %w", err)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer conn.Close()

	w, err := framestream.NewWriter(conn, &framestream.WriterOptions{
		ContentTypes:  [][]byte{dnstap.FSContentType},
		Bidirectional: true,
	})
	if err != nil {
		done <- fmt.Errorf("producer handshake: %w", err)
		return
	}

	for i := 0; i < n; i++ {
		frame := []byte(fmt.Sprintf("frame-%d-abcdefghijklmnopqrstuvwxyz", i))
		if _, err := w.WriteFrame(frame); err != nil {
			done <- fmt.Errorf("producer write: %w", err)
			return
		}
		if err := w.Flush(); err != nil {
			done <- fmt.Errorf("producer flush: %w", err)
			return
		}
	}
	time.Sleep(300 * time.Millisecond)
	done <- nil
}

// fakeCollector listens on a TCP address, performs the FSTRM handshake as a
// reader, and reports once it has received exactly want frames.
func fakeCollector(addr string, want int, done chan error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		done <- fmt.Errorf("collector listen: %w", err)
		return
	}
	defer ln.Close()

	conn, err := ln.Accept()
	if err != nil {
		done <- fmt.Errorf("collector accept: %w", err)
		return
	}
	defer conn.Close()

	r, err := framestream.NewReader(conn, &framestream.ReaderOptions{
		ContentTypes:  [][]byte{dnstap.FSContentType},
		Bidirectional: true,
		Timeout:       10 * time.Second,
	})
	if err != nil {
		done <- fmt.Errorf("collector handshake: %w", err)
		return
	}

	buf := make([]byte, 4096)
	count := 0
	for count < want {
		if _, err := r.ReadFrame(buf); err != nil {
			break
		}
		count++
	}
	if count != want {
		done <- fmt.Errorf("collector got %d frames, want %d", count, want)
		return
	}
	done <- nil
}

func TestRelayForwardsFrames(t *testing.T) {
	const frames = 500
	sock := filepath.Join(t.TempDir(), "dnstap.sock")
	tcpAddr := "127.0.0.1:0"

	// Reserve a free TCP port for the collector.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve collector port: %v", err)
	}
	tcpAddr = probe.Addr().String()
	probe.Close()

	producerDone := make(chan error, 1)
	collectorDone := make(chan error, 1)

	go fakeCollector(tcpAddr, frames, collectorDone)

	// Give the collector a moment to start listening.
	time.Sleep(300 * time.Millisecond)

	rl := New(Config{
		Input:             Endpoint{Type: "unix", Address: sock},
		Output:            Endpoint{Type: "tcp", Address: tcpAddr},
		QueueSize:         1000,
		ReconnectInterval: time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rl.Start()
	go fakeProducer(sock, frames, producerDone)

	var producerErr, collectorErr error
	select {
	case producerErr = <-producerDone:
	case <-time.After(15 * time.Second):
		producerErr = fmt.Errorf("producer timeout")
	}
	select {
	case collectorErr = <-collectorDone:
	case <-time.After(15 * time.Second):
		collectorErr = fmt.Errorf("collector timeout")
	}

	rl.Stop()

	if producerErr != nil {
		t.Fatalf("producer: %v", producerErr)
	}
	if collectorErr != nil {
		t.Fatalf("collector: %v", collectorErr)
	}
	if fwd, drop := rl.Metrics(); fwd != frames {
		t.Fatalf("relay forwarded %d frames, want %d", fwd, frames)
	} else if drop != 0 {
		t.Fatalf("unexpected dropped frames: %d", drop)
	}
}
