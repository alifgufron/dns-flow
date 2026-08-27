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

build: | $(BINDIR_OUT)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINDIR_OUT)/dns-flow ./cmd/dns-flow/
	@echo "built $(BINDIR_OUT)/dns-flow"

build-linux: | $(BINDIR_OUT)
	GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINDIR_OUT)/dns-flow-linux-amd64 ./cmd/dns-flow/
	@echo "built $(BINDIR_OUT)/dns-flow-linux-amd64"

build-freebsd: | $(BINDIR_OUT)
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
	@echo "FreeBSD install complete. Enable with: sysrc dns_flow_enable=YES"

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
	@echo "Linux install complete. Enable with: systemctl daemon-reload && systemctl enable --now dns-flow"

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
	rm -f $(DESTDIR)/usr/local/sbin/dns-flow
	rm -f $(DESTDIR)/usr/local/etc/dns-flow.yaml.sample
	rm -f $(DESTDIR)/etc/dns-flow.yaml.sample
	rm -f $(DESTDIR)/usr/local/etc/rc.d/dns-flow
	rm -f $(DESTDIR)/etc/systemd/system/dns-flow.service
	@echo "removed binary and service unit"

clean:
	rm -rf $(BINDIR_OUT)

