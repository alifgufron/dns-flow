.PHONY: all build build-freebsd install clean

BINARY   ?= bin/dns-flow-linux
BINARY_FREEBSD ?= bin/dns-flow-freebsd
PREFIX   ?= /usr/local

GO       ?= go
GOFLAGS  ?=
LDFLAGS  ?=

OS       := $(shell uname -s)

# OS-specific paths
ifeq ($(OS),FreeBSD)
CONFDIR  ?= $(PREFIX)/etc
BINDIR   ?= $(PREFIX)/sbin
RCDIR    ?= $(PREFIX)/etc/rc.d
DATADIR  ?= $(PREFIX)/var/db/dns-flow
else
CONFDIR  ?= /etc
BINDIR   ?= $(PREFIX)/sbin
UNITDIR  ?= /etc/systemd/system
DATADIR  ?= $(PREFIX)/var/lib/dns-flow
endif

all: build

build: | bin
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/dns-flow/

build-freebsd: | bin
	GOOS=freebsd GOARCH=amd64 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY_FREEBSD) ./cmd/dns-flow/

install: build
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 $(BINARY) $(DESTDIR)$(BINDIR)/dns-flow
	install -d $(DESTDIR)$(CONFDIR)
	install -m 644 configs/config.yaml $(DESTDIR)$(CONFDIR)/dns-flow.yaml
	install -d -m 755 $(DESTDIR)$(DATADIR)
ifeq ($(OS),FreeBSD)
	install -d $(DESTDIR)$(RCDIR)
	install -m 755 init/rc.d/dns-flow $(DESTDIR)$(RCDIR)/dns-flow
else
	install -d $(DESTDIR)$(UNITDIR)
	install -m 644 init/systemd/dns-flow.service $(DESTDIR)$(UNITDIR)/dns-flow.service
endif

bin:
	mkdir -p bin

clean:
	rm -rf bin
