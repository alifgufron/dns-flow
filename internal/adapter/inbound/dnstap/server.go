package dnstap

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	framestream "github.com/farsightsec/golang-framestream"
	"github.com/miekg/dns"

	"github.com/alifgufron/dns-flow/internal/domain"
)

// unixSocketMode is applied to the DNSTAP unix socket so a DNS source running
// under a different user but in the same group can connect to it.
const unixSocketMode = 0o660

type Config struct {
	Type       string
	Listen     string
	UnixSocket string
}

type Server struct {
	cfg      Config
	pipeline domain.Pipeline
	logger   *slog.Logger
	ln       net.Listener
	cancel   context.CancelFunc
}

func NewServer(cfg Config, pipeline domain.Pipeline, logger *slog.Logger) *Server {
	return &Server{
		cfg:      cfg,
		pipeline: pipeline,
		logger:   logger,
	}
}

func (s *Server) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	network := "tcp"
	addr := s.cfg.Listen
	if s.cfg.Type == "unix" {
		network = "unix"
		addr = s.cfg.UnixSocket
		os.Remove(addr)
	}

	var err error
	s.ln, err = net.Listen(network, addr)
	if err != nil {
		return err
	}

	if s.cfg.Type == "unix" {
		// The DNS source (e.g. BIND) connects to this socket and needs write
		// access. 0660 keeps it off-limits to other users; the source's user
		// must share the group that owns the socket.
		if err := os.Chmod(addr, unixSocketMode); err != nil {
			s.ln.Close()
			return fmt.Errorf("chmod dnstap socket %s: %w", addr, err)
		}
	}

	s.logger.Info("dnstap server listening", "address", addr, "type", network)

	go s.acceptLoop(ctx)
	return nil
}

func (s *Server) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.ln != nil {
		if err := s.ln.Close(); err != nil {
			return err
		}
	}
	if s.cfg.Type == "unix" {
		os.Remove(s.cfg.UnixSocket)
	}
	return nil
}

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				s.logger.Error("accept error", "error", err)
				continue
			}
		}
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	s.logger.Info("dnstap client connected", "remote", remoteAddr)

	reader, err := framestream.NewReader(conn, &framestream.ReaderOptions{
		ContentTypes:  [][]byte{dnstap.FSContentType},
		Bidirectional: true,
		Timeout:       10 * time.Second,
	})
	if err != nil {
		s.logger.Error("framestream handshake failed", "error", err)
		return
	}

	decoder := dnstap.NewDecoder(reader, 1048576)

	for {
		select {
		case <-ctx.Done():
			_ = conn.SetReadDeadline(time.Now())
			return
		default:
		}

		var tap dnstap.Dnstap
		if err := decoder.Decode(&tap); err != nil {
			s.logger.Info("dnstap decode done", "remote", remoteAddr, "error", err)
			return
		}

		event := s.decodeToEvent(&tap, remoteAddr)
		if event != nil {
			s.pipeline.Process(*event)
		}
	}
}

func (s *Server) decodeToEvent(tap *dnstap.Dnstap, remoteAddr string) *domain.DNSRawEvent {
	msg := tap.GetMessage()
	if msg == nil {
		return nil
	}

	event := &domain.DNSRawEvent{}

	// --- DNSTap Info ---
	event.DNSTap.Identity = string(tap.GetIdentity())
	event.DNSTap.Version = string(tap.GetVersion())
	event.DNSTap.Extra = string(tap.GetExtra())
	event.DNSTap.Type = tap.GetType().String()
	event.DNSTap.Operation = msg.GetType().String()
	if host, portStr, err := net.SplitHostPort(remoteAddr); err == nil {
		event.DNSTap.SocketIP = host
		if port, err := strconv.Atoi(portStr); err == nil {
			event.DNSTap.SocketPort = port
		}
	}
	if z := msg.GetQueryZone(); len(z) > 0 {
		event.DNSTap.QueryZone = string(z)
	}

	// --- Timestamp & Latency ---
	qSec := msg.GetQueryTimeSec()
	qNsec := msg.GetQueryTimeNsec()
	rSec := msg.GetResponseTimeSec()
	rNsec := msg.GetResponseTimeNsec()

	switch {
	case rSec > 0:
		event.DNSTap.Timestamp = time.Unix(int64(rSec), int64(rNsec))
		if qSec > 0 {
			qts := time.Unix(int64(qSec), int64(qNsec))
			dur := event.DNSTap.Timestamp.Sub(qts)
			event.DNSTap.Latency = dur.Seconds()
			event.DNSTap.LatencyMs = dur.Milliseconds()
		}
	case qSec > 0:
		event.DNSTap.Timestamp = time.Unix(int64(qSec), int64(qNsec))
	default:
		event.DNSTap.Timestamp = time.Now()
	}

	// --- Parse Extra field (DNSDist policy data) ---
	if event.DNSTap.Extra != "" {
		s.parseExtra(event.DNSTap.Extra, event)
	}

	// --- Network Info ---
	switch msg.GetSocketFamily() {
	case dnstap.SocketFamily_INET:
		event.Network.Family = "IPv4"
	case dnstap.SocketFamily_INET6:
		event.Network.Family = "IPv6"
	default:
		event.Network.Family = "unknown"
	}

	switch msg.GetSocketProtocol() {
	case dnstap.SocketProtocol_UDP:
		event.Network.Protocol = "UDP"
	case dnstap.SocketProtocol_TCP:
		event.Network.Protocol = "TCP"
	default:
		event.Network.Protocol = "unknown"
	}

	if addr := msg.GetQueryAddress(); len(addr) > 0 {
		event.Network.QueryIP = net.IP(addr).String()
	}
	event.Network.QueryPort = int(msg.GetQueryPort())

	if addr := msg.GetResponseAddress(); len(addr) > 0 {
		event.Network.ResponseIP = net.IP(addr).String()
	}
	event.Network.ResponsePort = int(msg.GetResponsePort())

	// --- DNS Query ---
	qMsg := msg.GetQueryMessage()
	if len(qMsg) > 0 {
		s.parseDNS(qMsg, event, true)
	}

	// --- DNS Response ---
	rMsg := msg.GetResponseMessage()
	if len(rMsg) > 0 {
		s.parseDNS(rMsg, event, false)
	}

	return event
}

func (s *Server) parseDNS(wire []byte, event *domain.DNSRawEvent, isQuery bool) {
	d := new(dns.Msg)
	if err := d.Unpack(wire); err != nil {
		event.DNS.Malformed = true
		return
	}

	h := &d.MsgHdr

	event.DNS.ID = int(h.Id)
	event.DNS.Opcode = h.Opcode
	event.DNS.Length = len(wire)
	event.DNS.Qdcount = len(d.Question)
	event.DNS.Ancount = len(d.Answer)
	event.DNS.Nscount = len(d.Ns)
	event.DNS.Arcount = len(d.Extra)

	event.DNS.Flags.QR = h.Response
	event.DNS.Flags.TC = h.Truncated
	event.DNS.Flags.AA = h.Authoritative
	event.DNS.Flags.RA = h.RecursionAvailable
	event.DNS.Flags.AD = h.AuthenticatedData
	event.DNS.Flags.RD = h.RecursionDesired
	event.DNS.Flags.CD = h.CheckingDisabled

	if !isQuery {
		event.DNS.RCode = dns.RcodeToString[h.Rcode]
	}

	if len(d.Question) > 0 {
		q := d.Question[0]
		event.DNS.QName = q.Name
		event.DNS.QType = dns.TypeToString[q.Qtype]
		event.DNS.QClass = dns.ClassToString[q.Qclass]
	}

	event.DNS.Resource = make(map[string][]domain.DNSRR)

	s.appendRR("an", d.Answer, event)
	s.appendRR("ns", d.Ns, event)
	s.appendRR("ar", d.Extra, event)

	s.parseEDNS(d.IsEdns0(), event)
}

func (s *Server) appendRR(section string, rrs []dns.RR, event *domain.DNSRawEvent) {
	for _, rr := range rrs {
		hdr := rr.Header()
		record := domain.DNSRR{
			Name:      hdr.Name,
			RDataType: dns.TypeToString[hdr.Rrtype],
			Class:     dns.ClassToString[hdr.Class],
			TTL:       int(hdr.Ttl),
			RData:     rdataToString(rr),
		}
		event.DNS.Resource[section] = append(event.DNS.Resource[section], record)
	}
}

func rdataToString(rr dns.RR) string {
	switch v := rr.(type) {
	case *dns.A:
		return v.A.String()
	case *dns.AAAA:
		return v.AAAA.String()
	case *dns.CNAME:
		return v.Target
	case *dns.MX:
		return fmt.Sprintf("%d %s", v.Preference, v.Mx)
	case *dns.NS:
		return v.Ns
	case *dns.PTR:
		return v.Ptr
	case *dns.SOA:
		return fmt.Sprintf("%s %s %d %d %d %d %d",
			v.Ns, v.Mbox, v.Serial, v.Refresh, v.Retry, v.Expire, v.Minttl)
	case *dns.TXT:
		return strings.Join(v.Txt, " ")
	case *dns.SRV:
		return fmt.Sprintf("%d %d %d %s", v.Priority, v.Weight, v.Port, v.Target)
	case *dns.DS:
		return fmt.Sprintf("%d %d %d %s", v.KeyTag, v.Algorithm, v.DigestType, v.Digest)
	case *dns.DNSKEY:
		return fmt.Sprintf("%d %d %d %s", v.Flags, v.Protocol, v.Algorithm, v.PublicKey)
	case *dns.RRSIG:
		return fmt.Sprintf("%s %d %d %d %d %d %d %s %s",
			dns.TypeToString[v.TypeCovered], v.Algorithm, v.Labels, v.OrigTtl,
			v.Expiration, v.Inception, v.KeyTag, v.SignerName, v.Signature)
	case *dns.NSEC:
		var sb strings.Builder
		sb.WriteString(v.NextDomain)
		for _, t := range v.TypeBitMap {
			fmt.Fprintf(&sb, " %s", dns.TypeToString[t])
		}
		return sb.String()
	case *dns.NSEC3:
		var sb strings.Builder
		fmt.Fprintf(&sb, "%d %d %d %s %s", v.Hash, v.Flags, v.Iterations, v.Salt, v.NextDomain)
		for _, t := range v.TypeBitMap {
			fmt.Fprintf(&sb, " %s", dns.TypeToString[t])
		}
		return sb.String()
	case *dns.CAA:
		return fmt.Sprintf("%d %s %s", v.Flag, v.Tag, v.Value)
	case *dns.LOC:
		return fmt.Sprintf("%d %d %d %d %d %d %d", v.Version, v.Size, v.HorizPre, v.VertPre, v.Latitude, v.Longitude, v.Altitude)
	case *dns.SSHFP:
		return fmt.Sprintf("%d %d %s", v.Algorithm, v.Type, v.FingerPrint)
	case *dns.TLSA:
		return fmt.Sprintf("%d %d %d %s", v.Usage, v.Selector, v.MatchingType, v.Certificate)
	case *dns.HTTPS:
		return svcbString(&v.SVCB)
	case *dns.SVCB:
		return svcbString(v)
	default:
		return rr.String()
	}
}

func svcbString(v *dns.SVCB) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d %s", v.Priority, v.Target)
	for _, kv := range v.Value {
		switch kv.Key() {
		case dns.SVCB_NO_DEFAULT_ALPN:
			sb.WriteString(" no-default-alpn")
		default:
			fmt.Fprintf(&sb, " %s=%s", kv.Key().String(), kv.String())
		}
	}
	return sb.String()
}

func (s *Server) parseEDNS(o *dns.OPT, event *domain.DNSRawEvent) {
	if o == nil {
		return
	}

	event.EDNS.Version = int(o.Version())
	event.EDNS.UDPSize = int(o.UDPSize())
	event.EDNS.RCode = int(o.ExtendedRcode())
	event.EDNS.DNSSECOK = o.Do()

	for _, opt := range o.Option {
		ednsOpt := domain.EDNSOption{
			Code:  int(opt.Option()),
			Value: opt.String(),
		}
		event.EDNS.Options = append(event.EDNS.Options, ednsOpt)

		switch v := opt.(type) {
		case *dns.EDNS0_SUBNET:
			event.EDNS.ClientSubnet = v.Address.String()
		}
	}
}

func (s *Server) parseExtra(extra string, event *domain.DNSRawEvent) {
	for _, field := range strings.Fields(extra) {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch k {
		case "peer-name":
			event.DNSTap.PeerName = v
		case "policy-rule":
			event.DNSTap.PolicyRule = v
		case "policy-type":
			event.DNSTap.PolicyType = v
		case "policy-match":
			event.DNSTap.PolicyMatch = v
		case "policy-value":
			event.DNSTap.PolicyValue = v
		}
	}
}
