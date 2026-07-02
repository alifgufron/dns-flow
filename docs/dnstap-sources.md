# DNSTAP Source Configuration

dns-flow receives DNSTAP framestream over TCP from any DNS server supporting the DNSTAP protocol ([RFC 8618](https://www.rfc-editor.org/info/rfc8618)).

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

If BIND does not support TCP output (older versions), write to a Unix socket and forward with `fstrm_capture`:

```text
options {
    dnstap { all; };
    dnstap-output unix "/var/run/named/dnstap.sock";
};
```

Then run a relay to forward to dns-flow:

```bash
fstrm_capture -t protobuf:dnstap.Dnstap -u /var/run/named/dnstap.sock -w /dev/stdout | \
  fstrm_sender -t protobuf:dnstap.Dnstap -u tcp://192.168.1.100:6000
```

Restart:

```bash
systemctl restart named
```

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
    dnstap-socket-path: "/var/run/unbound/dnstap.sock"
    dnstap-tls: no
    dnstap-log-client-query-messages: yes
    dnstap-log-client-response-messages: yes
```

Unbound only supports Unix socket output natively. Use a relay to forward to a remote collector:

```bash
fstrm_sender -t protobuf:dnstap.Dnstap -u tcp://192.168.1.100:6000 \
  -i /var/run/unbound/dnstap.sock
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
