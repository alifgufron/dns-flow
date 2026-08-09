// Package relay implements a stateless FSTRM frame relay.
//
// The relay reads Frame Streams frames from an input endpoint (e.g. a BIND
// DNSTAP unix socket) and forwards them, payload untouched, to an output
// endpoint (e.g. a remote dns-flow collector). Frames are buffered in an
// in-memory queue; when the queue is full new frames are dropped so that the
// input producer is never blocked.
package relay

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	framestream "github.com/farsightsec/golang-framestream"
)

// unixSocketMode is applied to a unix input socket so a producer running under
// a different user but in the same group can connect to it.
const unixSocketMode = 0o660

type Endpoint struct {
	Type    string
	Address string
}

type Config struct {
	Input             Endpoint
	Output            Endpoint
	QueueSize         int
	ReconnectInterval time.Duration
}

type Relay struct {
	cfg         Config
	logger      *slog.Logger
	queue       chan []byte
	done        chan struct{}
	wg          sync.WaitGroup
	contentType atomic.Value
	dropped     atomic.Uint64
	forwarded   atomic.Uint64
}

func New(cfg Config, logger *slog.Logger) *Relay {
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 100000
	}
	if cfg.ReconnectInterval <= 0 {
		cfg.ReconnectInterval = 5 * time.Second
	}
	return &Relay{
		cfg:    cfg,
		logger: logger,
		queue:  make(chan []byte, queueSize),
		done:   make(chan struct{}),
	}
}

// Start launches the reader and writer loops. It returns immediately.
func (r *Relay) Start() {
	r.wg.Add(2)
	go r.readerLoop()
	go r.writerLoop()
	r.logger.Info("relay started",
		"input", fmt.Sprintf("%s:%s", r.cfg.Input.Type, r.cfg.Input.Address),
		"output", fmt.Sprintf("%s:%s", r.cfg.Output.Type, r.cfg.Output.Address),
		"queue_size", cap(r.queue),
	)
}

// Stop shuts down the relay and waits for all loops to exit.
func (r *Relay) Stop() {
	close(r.done)
	r.wg.Wait()
	r.logger.Info("relay stopped",
		"forwarded", r.forwarded.Load(),
		"dropped", r.dropped.Load(),
	)
}

// dial connects to a tcp endpoint, or to a unix socket owned by a remote
// listener (e.g. the relay output or a remote source for a tcp input).
func (r *Relay) dial(ep Endpoint) (net.Conn, error) {
	if ep.Type == "unix" {
		return net.DialTimeout("unix", ep.Address, 10*time.Second)
	}
	return net.DialTimeout("tcp", ep.Address, 10*time.Second)
}

// readerLoop connects to the input, negotiates the FSTRM content type, and
// pushes every data frame into the queue. For a unix input the relay listens
// on the socket and accepts the producer's connection (e.g. BIND dials in);
// for a tcp input it dials the remote source. It reconnects forever until
// stopped.
func (r *Relay) readerLoop() {
	defer r.wg.Done()

	buf := make([]byte, 262144)
	var ln net.Listener

	defer func() {
		if ln != nil {
			ln.Close()
		}
		if r.cfg.Input.Type == "unix" {
			os.Remove(r.cfg.Input.Address)
		}
	}()

	for {
		select {
		case <-r.done:
			return
		default:
		}

		conn, err := r.openInput(&ln)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Accept timed out waiting for a producer to dial in.
				if !r.sleep() {
					return
				}
				continue
			}
			r.logger.Warn("relay input connect failed",
				"endpoint", r.cfg.Input.Address, "error", err)
			if !r.sleep() {
				return
			}
			continue
		}

		reader, err := framestream.NewReader(conn, &framestream.ReaderOptions{
			ContentTypes:  [][]byte{dnstap.FSContentType},
			Bidirectional: true,
			Timeout:       10 * time.Second,
		})
		if err != nil {
			r.logger.Warn("relay input handshake failed",
				"endpoint", r.cfg.Input.Address, "error", err)
			conn.Close()
			if !r.sleep() {
				return
			}
			continue
		}

		r.contentType.Store(reader.ContentType())
		r.logger.Info("relay input connected",
			"endpoint", r.cfg.Input.Address,
			"content_type", string(reader.ContentType()))

		for {
			select {
			case <-r.done:
				conn.Close()
				return
			default:
			}

			n, err := reader.ReadFrame(buf)
			if err != nil {
				r.logger.Info("relay input disconnected",
					"endpoint", r.cfg.Input.Address, "error", err)
				conn.Close()
				break
			}

			frame := make([]byte, n)
			copy(frame, buf[:n])
			select {
			case r.queue <- frame:
			default:
				dropped := r.dropped.Add(1)
				if dropped%1000 == 1 {
					r.logger.Warn("relay queue full, dropping frames",
						"dropped", dropped, "queue_size", cap(r.queue))
				}
			}
		}

		if !r.sleep() {
			return
		}
	}
}

// openInput returns a connection to the input endpoint. For a unix endpoint it
// creates and listens on the socket, then blocks until a producer (e.g. BIND)
// dials in; the listener is kept open so the producer can reconnect. For a
// tcp endpoint it dials the remote source.
func (r *Relay) openInput(ln *net.Listener) (net.Conn, error) {
	if r.cfg.Input.Type == "unix" {
		if *ln == nil {
			os.Remove(r.cfg.Input.Address)
			l, err := net.Listen("unix", r.cfg.Input.Address)
			if err != nil {
				return nil, err
			}
			// The producer (e.g. BIND) connects to this socket and needs write
			// access; 0660 requires it to share the socket's group.
			if err := os.Chmod(r.cfg.Input.Address, unixSocketMode); err != nil {
				l.Close()
				return nil, fmt.Errorf("chmod relay input socket %s: %w",
					r.cfg.Input.Address, err)
			}
			*ln = l
			r.logger.Info("relay input listening", "socket", r.cfg.Input.Address)
		}
		if u, ok := (*ln).(*net.UnixListener); ok {
			u.SetDeadline(time.Now().Add(r.cfg.ReconnectInterval))
		}
		return (*ln).Accept()
	}
	return r.dial(r.cfg.Input)
}

// writerLoop connects to the output and forwards queued frames using the
// content type negotiated by the reader. It reconnects forever until stopped.
func (r *Relay) writerLoop() {
	defer r.wg.Done()

	for {
		select {
		case <-r.done:
			return
		default:
		}

		ct, ok := r.contentType.Load().([]byte)
		if !ok || len(ct) == 0 {
			// Wait until the reader has negotiated a content type.
			if !r.sleep() {
				return
			}
			continue
		}

		conn, err := r.dial(r.cfg.Output)
		if err != nil {
			r.logger.Warn("relay output connect failed",
				"endpoint", r.cfg.Output.Address, "error", err)
			if !r.sleep() {
				return
			}
			continue
		}

		writer, err := framestream.NewWriter(conn, &framestream.WriterOptions{
			ContentTypes:  [][]byte{ct},
			Bidirectional: true,
			Timeout:       10 * time.Second,
		})
		if err != nil {
			r.logger.Warn("relay output handshake failed",
				"endpoint", r.cfg.Output.Address, "error", err)
			conn.Close()
			if !r.sleep() {
				return
			}
			continue
		}

		r.logger.Info("relay output connected", "endpoint", r.cfg.Output.Address)

		for {
			select {
			case <-r.done:
				writer.Close()
				conn.Close()
				return
			case frame := <-r.queue:
				if _, err := writer.WriteFrame(frame); err != nil {
					r.logger.Warn("relay output write failed, reconnecting",
						"endpoint", r.cfg.Output.Address, "error", err)
					writer.Close()
					conn.Close()
					goto reconnect
				}
				if err := writer.Flush(); err != nil {
					r.logger.Warn("relay output flush failed, reconnecting",
						"endpoint", r.cfg.Output.Address, "error", err)
					writer.Close()
					conn.Close()
					goto reconnect
				}
				r.forwarded.Add(1)
			}
		}

	reconnect:
		if !r.sleep() {
			return
		}
	}
}

// sleep waits for the reconnect interval or until the relay is stopped.
// It returns false when the relay is stopped.
func (r *Relay) sleep() bool {
	select {
	case <-r.done:
		return false
	case <-time.After(r.cfg.ReconnectInterval):
		return true
	}
}

// Metrics returns the current forwarded and dropped frame counters.
func (r *Relay) Metrics() (forwarded, dropped uint64) {
	return r.forwarded.Load(), r.dropped.Load()
}
