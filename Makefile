.PHONY: all build build-linux build-freebsd build-all install install-service \
        create-user uninstall test vet fmt clean help

# --- Toolchain ---
GO       ?= go
GOFLAGS  ?=
LDFLAGS  ?=

BINDIR_OUT ?= bin

# --- Build ---
all: build

help:
	@echo "Targets:"
	@echo "  build           Build binary for current target OS/arch"
	@echo "  build-linux     Cross-build linux/amd64"
	@echo "  build-freebsd   Cross-build freebsd/amd64"
	@echo "  build-all       Build linux + freebsd"
	@echo "  install         Install binary, config, service unit"
	@echo "  create-user     Create dnsflow system user"
	@echo "  uninstall       Remove installed files (config is kept)"
	@echo "  test / vet / fmt / clean"

$(BINDIR_OUT):
	mkdir -p $(BINDIR_OUT)

build:
	@mkdir -p $(BINDIR_OUT)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINDIR_OUT)/dns-flow ./cmd/dns-flow/
	@echo "built $(BINDIR_OUT)/dns-flow"

build-linux:
	@mkdir -p $(BINDIR_OUT)
	GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINDIR_OUT)/dns-flow-linux-amd64 ./cmd/dns-flow/
	@echo "built $(BINDIR_OUT)/dns-flow-linux-amd64"

build-freebsd:
	@mkdir -p $(BINDIR_OUT)
	GOOS=freebsd GOARCH=amd64 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINDIR_OUT)/dns-flow-freebsd-amd64 ./cmd/dns-flow/
	@echo "built $(BINDIR_OUT)/dns-flow-freebsd-amd64"

build-all: build-linux build-freebsd

# --- Quality gates ---
test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

# --- Install ---
SERVICE_USER  ?= dnsflow
SERVICE_GROUP ?= dnsflow

install: build
	@UNAME=$$(uname -s); \
	if [ "$$UNAME" = "FreeBSD" ]; then \
		$(MAKE) install-freebsd; \
	else \
		$(MAKE) install-linux; \
	fi

install-freebsd: create-user-freebsd
	install -d $(DESTDIR)/usr/local/sbin
	install -m 755 $(BINDIR_OUT)/dns-flow $(DESTDIR)/usr/local/sbin/dns-flow
	install -d $(DESTDIR)/usr/local/etc
	install -m 640 configs/config.yaml $(DESTDIR)/usr/local/etc/dns-flow.yaml.sample
	@if [ ! -f "$(DESTDIR)/usr/local/etc/dns-flow.yaml" ]; then \
		install -m 640 configs/config.yaml $(DESTDIR)/usr/local/etc/dns-flow.yaml; \
		echo "installed /usr/local/etc/dns-flow.yaml"; \
	fi
	install -d -m 750 $(DESTDIR)/var/db/dns-flow
	install -d -m 750 $(DESTDIR)/var/log/dns-flow
	install -d $(DESTDIR)/usr/local/etc/rc.d
	install -m 755 init/rc.d/dns-flow $(DESTDIR)/usr/local/etc/rc.d/dns-flow
	@echo ""
	@echo "========================================================================"
	@echo "  FreeBSD install complete!"
	@echo "========================================================================"
	@echo "  1. Config file sample : /usr/local/etc/dns-flow.yaml.sample"
	@echo "  2. Active config file : /usr/local/etc/dns-flow.yaml"
	@echo "     (Please edit /usr/local/etc/dns-flow.yaml to configure settings)"
	@echo "  3. Enable service     : sysrc dns_flow_enable=YES"
	@echo "  4. Start service      : service dns-flow start"
	@echo "========================================================================"

install-linux: create-user-linux
	install -d $(DESTDIR)/usr/local/sbin
	install -m 755 $(BINDIR_OUT)/dns-flow $(DESTDIR)/usr/local/sbin/dns-flow
	install -d $(DESTDIR)/etc
	install -m 640 configs/config.yaml $(DESTDIR)/etc/dns-flow.yaml.sample
	@if [ ! -f "$(DESTDIR)/etc/dns-flow.yaml" ]; then \
		install -m 640 configs/config.yaml $(DESTDIR)/etc/dns-flow.yaml; \
		echo "installed /etc/dns-flow.yaml"; \
	fi
	install -d -m 750 $(DESTDIR)/var/lib/dns-flow
	install -d -m 750 $(DESTDIR)/var/log/dns-flow
	install -d $(DESTDIR)/etc/systemd/system
	install -m 644 init/systemd/dns-flow.service $(DESTDIR)/etc/systemd/system/dns-flow.service
	@echo ""
	@echo "========================================================================"
	@echo "  Linux install complete!"
	@echo "========================================================================"
	@echo "  1. Config file sample : /etc/dns-flow.yaml.sample"
	@echo "  2. Active config file : /etc/dns-flow.yaml"
	@echo "     (Please edit /etc/dns-flow.yaml to configure settings)"
	@echo "  3. Enable & start     : systemctl daemon-reload && systemctl enable --now dns-flow"
	@echo "========================================================================"

create-user-freebsd:
	@if [ -z "$(DESTDIR)" ]; then \
		pw groupadd $(SERVICE_GROUP) 2>/dev/null || true; \
		pw adduser $(SERVICE_USER) -g $(SERVICE_GROUP) -d /nonexistent -s /usr/sbin/nologin -c "dns-flow service user" 2>/dev/null || true; \
	fi

create-user-linux:
	@if [ -z "$(DESTDIR)" ]; then \
		groupadd --system $(SERVICE_GROUP) 2>/dev/null || true; \
		useradd --system --gid $(SERVICE_GROUP) --home-dir /var/lib/dns-flow --no-create-home --shell /usr/sbin/nologin --comment "dns-flow service user" $(SERVICE_USER) 2>/dev/null || true; \
	fi

uninstall:
	@UNAME=$$(uname -s); \
	if [ "$$UNAME" = "FreeBSD" ]; then \
		$(MAKE) uninstall-freebsd; \
	else \
		$(MAKE) uninstall-linux; \
	fi

uninstall-freebsd:
	rm -f $(DESTDIR)/usr/local/sbin/dns-flow
	rm -f $(DESTDIR)/usr/local/etc/dns-flow.yaml.sample
	rm -f $(DESTDIR)/usr/local/etc/rc.d/dns-flow
	@echo "Removed binary, sample config, and rc.d service script."
	@echo "Preserved active config (/usr/local/etc/dns-flow.yaml) and data directory."

uninstall-linux:
	rm -f $(DESTDIR)/usr/local/sbin/dns-flow
	rm -f $(DESTDIR)/etc/dns-flow.yaml.sample
	rm -f $(DESTDIR)/etc/systemd/system/dns-flow.service
	@echo "Removed binary, sample config, and systemd service unit."
	@echo "Preserved active config (/etc/dns-flow.yaml) and data directory."

clean:
	rm -rf $(BINDIR_OUT)

