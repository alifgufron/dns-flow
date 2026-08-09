# DNSTAP Source Configuration

dns-flow receives DNSTAP framestream over TCP or from a Unix socket from any DNS server supporting the DNSTAP protocol ([RFC 8618](https://www.rfc-editor.org/info/rfc8618)).

- `dnstap.type: tcp` (default) — dns-flow listens on `dnstap.listen`; sources dial into it.
- `dnstap.type: unix` — dns-flow listens on the Unix socket (`dnstap.unix_socket`); sources (BIND, Unbound) connect to it.
- `mode: relay` — dns-flow forwards untouched FSTRM frames from `relay.input` to `relay.output`.

Connection direction: in both TCP and Unix socket cases, the DNS server acts as the **client** and dns-flow acts as the **server** that creates the socket/listener and accepts connections (the same role as `fstrm_capture`).

Replace `/path/to/dnstap.sock` in the examples below with a path inside the
runtime directory created by the service unit — `/run/dns-flow/dnstap.sock` on
Linux, `/var/run/dns-flow/dnstap.sock` on FreeBSD. dns-flow creates the socket
with mode `0660`, so the DNS server's user must be in the `dnsflow` group; see
[Unix socket permissions](usage.md#unix-socket-permissions).

## DNSDist

In `/etc/dnsdist/dnsdist.conf`:

```lua
-- FrameStream TCP logger targeting dns-flow
fstr = newFrameStreamTcpLogger("10.0.0.1:6000")

-- Log all queries
addAction(
  AllRule(),
  DnstapLogAction("dnsdist-01", fstr)
)

-- Log all responses
addResponseAction(
  AllRule(),
  DnstapLogResponseAction("dnsdist-01", fstr)
)
```

Restart:

```bash
systemctl restart dnsdist
```

The identity string (e.g. `"dnsdist-01"`) appears in the `dnstap.identity` field of each event.

## BIND

BIND 9.11+ supports DNSTAP when compiled with `--enable-dnstap`. See [ISC KB aa-01342](https://kb.isc.org/docs/aa-01342) for details.

In `named.conf`:

```text
options {
    dnstap { client query client-response auth query auth-response resolver query resolver-response forwarder query forwarder-response; };
    dnstap-output tcp://192.168.1.100:6000;
    dnstap-identity "ns1-main";
    dnstap-version "9.18";
};
```

### BIND with Unix socket

For older BIND versions without TCP DNSTAP output (or to avoid exposing the collector over TCP), BIND can connect to a local Unix socket created by dns-flow:

```text
options {
    dnstap { all; };
    dnstap-output unix "/path/to/dnstap.sock";
};
```

Collector config (`mode: collect`) that listens on the socket and stores locally:

```yaml
mode: collect
dnstap:
  type: unix
  unix_socket: "/path/to/dnstap.sock"
```

Relay config (`mode: relay`) that listens on the socket and forwards the untouched FSTRM frames to a remote collector:

```yaml
mode: relay
relay:
  input:
    type: unix
    address: "/path/to/dnstap.sock"
  output:
    type: tcp
    address: "192.168.1.100:6000"
```

Restart:

```bash
systemctl restart named
```

dns-flow creates the socket file, so the socket directory must be writable by the user running dns-flow, and the `named` user must be allowed to connect to it (e.g. same user, or a shared group with appropriate permissions on the directory).

## PowerDNS

In `pdns.conf`:

```text
dnstap=yes
dnstap-url=tcp://192.168.1.100:6000
dnstap-logResponses=yes
dnstap-logQueries=yes
dnstap-identity=pdns1
```

Restart:

```bash
systemctl restart pdns
```

## Unbound

Unbound requires compilation with `--enable-dnstap`. In `unbound.conf`:

```text
server:
    dnstap:
    dnstap-socket-path: "/path/to/dnstap.sock"
    dnstap-tls: no
    dnstap-log-client-query-messages: yes
    dnstap-log-client-response-messages: yes
```

Unbound only supports Unix socket output natively and connects to a socket created by dns-flow. Use dns-flow relay mode to forward to a remote collector:

```yaml
mode: relay
relay:
  input:
    type: unix
    address: "/path/to/dnstap.sock"
  output:
    type: tcp
    address: "192.168.1.100:6000"
```

Or read the socket directly in collector mode and store locally:

```yaml
mode: collect
dnstap:
  type: unix
  unix_socket: "/path/to/dnstap.sock"
```

## Verification

Check the dns-flow log for incoming connections:

```
level=INFO msg="dnstap client connected" remote=192.168.1.1:34567
```

If no connection appears, verify:
- Firewall allows TCP port 6000 to the dns-flow host
- DNSTAP configuration in the DNS server is correct and the service has been restarted
- The DNS server can reach the dns-flow collector over the network

The `dnstap.version` field in events shows the sending software (e.g. `"dnsdist 2.0.6"`), making it easy to identify the source.
