.PHONY: all build build-linux build-freebsd build-all install install-service \
        create-user uninstall test vet fmt clean help

# --- Toolchain ---
GO       ?= go
GOFLAGS  ?=
LDFLAGS  ?=

# --- Host detection ---
HOST_OS   := $(shell uname -s | tr '[:upper:]' '[:lower:]')
HOST_ARCH := $(shell uname -m)
ifeq ($(HOST_ARCH),x86_64)
GOARCH ?= amd64
else ifeq ($(HOST_ARCH),aarch64)
GOARCH ?= arm64
else ifeq ($(HOST_ARCH),arm64)
GOARCH ?= arm64
else
GOARCH ?= $(HOST_ARCH)
endif

# GOOS to build/install for. Defaults to the host OS so `make install`
# always installs the artefacts matching the running system.
GOOS ?= $(HOST_OS)

BINDIR_OUT ?= bin
BINARY     ?= $(BINDIR_OUT)/dns-flow-$(GOOS)-$(GOARCH)

# --- Install layout (per target OS) ---
SERVICE_USER  ?= dnsflow
SERVICE_GROUP ?= dnsflow

ifeq ($(GOOS),freebsd)
PREFIX   ?= /usr/local
CONFDIR  ?= $(PREFIX)/etc
SBINDIR  ?= $(PREFIX)/sbin
RCDIR    ?= $(PREFIX)/etc/rc.d
DATADIR  ?= /var/db/dns-flow
RUNDIR   ?= /var/run/dns-flow
LOGDIR   ?= /var/log/dns-flow
CONFNAME ?= dns-flow.yaml
else
PREFIX   ?= /usr/local
CONFDIR  ?= /etc
SBINDIR  ?= $(PREFIX)/sbin
UNITDIR  ?= /etc/systemd/system
DATADIR  ?= /var/lib/dns-flow
RUNDIR   ?= /run/dns-flow
LOGDIR   ?= /var/log/dns-flow
CONFNAME ?= dns-flow.yaml
endif

CONFFILE := $(DESTDIR)$(CONFDIR)/$(CONFNAME)

all: build

help:
	@echo "Targets:"
	@echo "  build           Build for the host OS/arch ($(GOOS)/$(GOARCH))"
	@echo "  build-linux     Cross-build linux/amd64"
	@echo "  build-freebsd   Cross-build freebsd/amd64"
	@echo "  build-all       Build linux + freebsd"
	@echo "  install         Install binary, config, service unit ($(GOOS))"
	@echo "  create-user     Create the $(SERVICE_USER) system user"
	@echo "  uninstall       Remove installed files (config is kept)"
	@echo "  test / vet / fmt / clean"

# --- Build ---
$(BINDIR_OUT):
	mkdir -p $(BINDIR_OUT)

build: | $(BINDIR_OUT)
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/dns-flow/
	@echo "built $(BINARY)"

build-linux:
	$(MAKE) build GOOS=linux GOARCH=amd64

build-freebsd:
	$(MAKE) build GOOS=freebsd GOARCH=amd64

build-all: build-linux build-freebsd

# --- Quality gates ---
test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

# --- Install ---
# Creates the service user, installs the binary, installs the config only if
# it does not exist yet (an updated template is always written as .sample),
# then installs the service unit for the target OS.
install: build create-user
	install -d $(DESTDIR)$(SBINDIR)
	install -m 755 $(BINARY) $(DESTDIR)$(SBINDIR)/dns-flow

	install -d $(DESTDIR)$(CONFDIR)
	install -m 640 configs/config.yaml $(CONFFILE).sample
	@if [ -f "$(CONFFILE)" ]; then \
		echo "keeping existing $(CONFFILE) (template: $(CONFFILE).sample)"; \
	else \
		install -m 640 configs/config.yaml $(CONFFILE); \
		echo "installed $(CONFFILE)"; \
	fi
	-chgrp $(SERVICE_GROUP) $(CONFFILE) $(CONFFILE).sample 2>/dev/null || true

	install -d -m 750 $(DESTDIR)$(DATADIR)
	install -d -m 750 $(DESTDIR)$(LOGDIR)
	-chown $(SERVICE_USER):$(SERVICE_GROUP) $(DESTDIR)$(DATADIR) $(DESTDIR)$(LOGDIR) 2>/dev/null || true

	$(MAKE) install-service GOOS=$(GOOS)

ifeq ($(GOOS),freebsd)
install-service:
	install -d $(DESTDIR)$(RCDIR)
	install -m 755 init/rc.d/dns-flow $(DESTDIR)$(RCDIR)/dns-flow
	@echo ""
	@echo "Enable with:  sysrc dns_flow_enable=YES"
	@echo "Start with:   service dns-flow start"
else
install-service:
	install -d $(DESTDIR)$(UNITDIR)
	install -m 644 init/systemd/dns-flow.service $(DESTDIR)$(UNITDIR)/dns-flow.service
	@echo ""
	@echo "Enable with:  systemctl daemon-reload && systemctl enable --now dns-flow"
endif

# --- Service user ---
# Skipped when staging into DESTDIR (package build).
ifeq ($(GOOS),freebsd)
create-user:
	@if [ -n "$(DESTDIR)" ]; then \
		echo "DESTDIR set, skipping user creation"; \
	elif pw groupshow $(SERVICE_GROUP) >/dev/null 2>&1; then \
		echo "group $(SERVICE_GROUP) exists"; \
	else \
		pw groupadd $(SERVICE_GROUP) && echo "created group $(SERVICE_GROUP)"; \
	fi
	@if [ -n "$(DESTDIR)" ]; then \
		:; \
	elif pw usershow $(SERVICE_USER) >/dev/null 2>&1; then \
		echo "user $(SERVICE_USER) exists"; \
	else \
		pw adduser $(SERVICE_USER) -g $(SERVICE_GROUP) -d /nonexistent \
		   -s /usr/sbin/nologin -c "dns-flow service user" && \
		echo "created user $(SERVICE_USER)"; \
	fi
else
create-user:
	@if [ -n "$(DESTDIR)" ]; then \
		echo "DESTDIR set, skipping user creation"; \
	elif getent group $(SERVICE_GROUP) >/dev/null 2>&1; then \
		echo "group $(SERVICE_GROUP) exists"; \
	else \
		groupadd --system $(SERVICE_GROUP) && echo "created group $(SERVICE_GROUP)"; \
	fi
	@if [ -n "$(DESTDIR)" ]; then \
		:; \
	elif getent passwd $(SERVICE_USER) >/dev/null 2>&1; then \
		echo "user $(SERVICE_USER) exists"; \
	else \
		useradd --system --gid $(SERVICE_GROUP) --home-dir $(DATADIR) \
		        --no-create-home --shell /usr/sbin/nologin \
		        --comment "dns-flow service user" $(SERVICE_USER) && \
		echo "created user $(SERVICE_USER)"; \
	fi
endif

# --- Uninstall (config and data are preserved) ---
uninstall:
	rm -f $(DESTDIR)$(SBINDIR)/dns-flow
	rm -f $(CONFFILE).sample
ifeq ($(GOOS),freebsd)
	rm -f $(DESTDIR)$(RCDIR)/dns-flow
else
	rm -f $(DESTDIR)$(UNITDIR)/dns-flow.service
endif
	@echo "removed binary and service unit"
	@echo "kept: $(CONFFILE), $(DESTDIR)$(DATADIR), user $(SERVICE_USER)"

clean:
	rm -rf $(BINDIR_OUT)
