package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type Pipeline struct {
	storages    []domain.Storage
	geoip       domain.GeoIPResolver
	correlator  *QueryCorrelator
	workers     int
	queueSize   int
	queue       chan domain.DNSRawEvent
	cancel      context.CancelFunc
	die         chan struct{}
	wg          sync.WaitGroup
	logger      *slog.Logger
}

func NewPipeline(
	storages []domain.Storage,
	geoip domain.GeoIPResolver,
	workers int,
	queueSize int,
	logger *slog.Logger,
) *Pipeline {
	p := &Pipeline{
		storages:  storages,
		geoip:     geoip,
		workers:   workers,
		queueSize: queueSize,
		queue:     make(chan domain.DNSRawEvent, queueSize),
		die:       make(chan struct{}),
		logger:    logger,
	}
	p.correlator = NewQueryCorrelator(logger)
	return p
}

func (p *Pipeline) Process(event domain.DNSRawEvent) error {
	defer func() { recover() }()

	select {
	case <-p.die:
	case p.queue <- event:
	default:
		p.logger.Warn("pipeline queue full, dropping event")
	}
	return nil
}

func (p *Pipeline) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	p.correlator.Start()

	for range p.workers {
		p.wg.Add(1)
		go p.worker(ctx)
	}

	p.logger.Info("pipeline started",
		"workers", p.workers,
		"queue_size", p.queueSize,
		"output", "kafka",
	)
	return nil
}

func (p *Pipeline) Shutdown() error {
	if p.cancel != nil {
		p.cancel()
	}
	close(p.die)
	close(p.queue)
	p.wg.Wait()

	p.correlator.Stop()

	for _, s := range p.storages {
		if err := s.Close(); err != nil {
			p.logger.Error("storage close error", "name", s.Name(), "error", err)
		}
	}

	p.logger.Info("pipeline shut down gracefully")
	return nil
}

func (p *Pipeline) Health() map[string]string {
	return map[string]string{
		"pipeline": "running",
		"queue":    fmt.Sprintf("%d/%d", len(p.queue), p.queueSize),
	}
}

func (p *Pipeline) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-p.queue:
			if !ok {
				return
			}
			p.processEvent(ctx, event)
		}
	}
}

func (p *Pipeline) processEvent(ctx context.Context, event domain.DNSRawEvent) {
	if event.DNS.Flags.QR {
		if !p.correlator.Lookup(event) {
			p.logger.Debug("orphaned response (no matching query)",
				"query_ip", event.Network.QueryIP,
				"dns_id", event.DNS.ID,
				"qname", event.DNS.QName,
			)
		}
	} else {
		p.correlator.Store(event)
	}

	if p.geoip != nil {
		geo, err := p.geoip.Lookup(event.Network.QueryIP)
		if err != nil {
			p.logger.Warn("geoip lookup failed", "error", err)
		} else {
			event.GeoIP.ClientIP = geo.ClientIP
			event.GeoIP.ClientCountry = geo.ClientCountry
			event.GeoIP.ClientCity = geo.ClientCity
			event.GeoIP.ClientASN = geo.ClientASN
		}

		for _, section := range event.DNS.Resource {
			for i := range section {
				rr := &section[i]
				ip := rr.RData
				if rr.RDataType != "A" && rr.RDataType != "AAAA" {
					continue
				}
				if net.ParseIP(ip) == nil {
					continue
				}
				country, city, asn := p.geoip.LookupIP(ip)
				if country != "" || city != "" || asn != "" {
					rr.Geo = &domain.RRGeoInfo{
						Country: country,
						City:    city,
						ASN:     asn,
					}
				}
			}
		}
	}

	for _, s := range p.storages {
		if err := s.Write(event); err != nil {
			p.logger.Error("storage write error",
				"name", s.Name(),
				"error", err,
			)
		}
	}
}
