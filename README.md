# cfipd
[简体中文](README.zh.md)

cfipd is a relay for **HTTPS connections to Cloudflare-backed sites**, with
adaptive edge IP selection. It gives clients one connection endpoint while
continuously testing Cloudflare IPs, checking health, and managing replacements.

Clients retain the original HTTPS hostname and TLS SNI. Their TLS session runs
end to end with Cloudflare; cfipd forwards the encrypted stream without decrypting
or rewriting HTTP requests.

It supports IPv4 and IPv6, adapts probing frequency to network conditions, and
includes a read-only web dashboard. Release binaries are static, and Debian
packages include systemd services. The default target regions are Tokyo, Osaka,
Hong Kong, and Singapore; they can be changed in the configuration.

## No QUIC support

cfipd relays TCP only. It does not forward HTTP/3 (QUIC), which runs over UDP
port 443. Its edge selection measures TCP and TLS, and a UDP relay that faces the
internet can reflect amplified QUIC traffic. Clients that try HTTP/3 first fall
back to TCP when UDP fails. To make the fallback immediate, reject UDP port 443
to Cloudflare addresses on the client or the router.

## Quick start

On Debian or Ubuntu with systemd, download the `.deb` matching your machine from
[GitHub Releases](https://github.com/breakertt/cfipd/releases/latest). Run these
commands in the directory containing the downloaded package:

```bash
sudo apt install ./cfipd_*_"$(dpkg --print-architecture)".deb
sudo cfipd status

# Optional: enable the dashboard at http://127.0.0.1:8080
sudo systemctl enable --now cfipd-web
```

The daemon starts automatically and accepts relayed HTTPS connections on port
**8443** by default.
One daemon has one shared listener: all client connections use the same relay
address and port, and each new connection is forwarded to one selected Active
IP on port 443. Three Active IPs still need only one listening port. For example,
three separate HTTPS connections can be routed as follows:

```text
HTTPS connection A --> cfipd:8443 --> Active IP 1:443
HTTPS connection B --> cfipd:8443 --> Active IP 2:443
HTTPS connection C --> cfipd:8443 --> Active IP 1:443
```

Adding connections or replacing an Active IP does not open another listener. A second
independent daemon needs a different bind address or port if the first already
occupies it, plus its own data directory and control socket.

Allow it to discover and qualify endpoints; `sudo cfipd actives` shows when they
are available. To send an HTTPS request through the local relay:

```bash
curl --noproxy '*' \
  --connect-to speed.cloudflare.com:443:127.0.0.1:8443 \
  https://speed.cloudflare.com/cdn-cgi/trace
```

Configure the client's connection destination as in this example, preserving the
original HTTPS hostname and TLS SNI. The listener passes the TLS stream directly;
it does not perform an HTTP proxy `CONNECT` handshake. For other target regions,
edit `allowed_colos` in `/etc/cfipd.json` and restart the daemon.

## Installation and service management

Releases contain a standalone binary and a `.deb` for each supported Linux target:

| CPU | Binary suffix | Debian architecture |
|---|---|---|
| x86-64 | `linux_amd64` | `amd64` (baseline x86-64 / GOAMD64=v1) |
| ARM64 | `linux_arm64` | `arm64` |
| ARMv7 | `linux_armv7` | `armhf` |
| 32-bit x86 | `linux_386` | `i386` |
| RISC-V 64 | `linux_riscv64` | `riscv64` |

`SHA256SUMS` accompanies every release. Standalone binaries require no Go runtime
or libc; make the downloaded file executable and use its `run` subcommand. HTTPS
probing requires the system CA certificate bundle.

The Debian package runs the daemon as the dedicated `cfipd` system user:

| Item | Location |
|---|---|
| Configuration | `/etc/cfipd.json` |
| Learned state and probe history | `/var/lib/cfipd/` |
| Control socket | `/run/cfipd/ctl.sock` |
| Daemon / dashboard services | `cfipd.service` / `cfipd-web.service` |

```bash
cfipd test -config /etc/cfipd.json    # Check configuration changes
sudo systemctl restart cfipd          # Apply configuration changes
systemctl status cfipd
sudo journalctl -u cfipd -f
sudo systemctl disable --now cfipd-web
```

Upgrades preserve local configuration and administrator-disabled services.
Removal retains learned state and the service account; purging also removes the
packaged configuration. Before installing over a manually managed instance,
back up its data and stop it. Two daemons must not share a data directory.

## Configuration

cfipd reads one JSON file, chosen in this order:

1. The file named by `-config`, for example `cfipd run -config /path/to/config.json`.
2. The file named by the `CFIPD_CONFIG_PATH` environment variable.
3. `cfipd.json` in the current directory, then `/etc/cfipd.json`.
4. Built-in defaults when none of these files exists.

The systemd services pass `-config /etc/cfipd.json`, so `CFIPD_CONFIG_PATH` does
not change them. Fields in the file override built-in defaults. The package
installs this `/etc/cfipd.json`:

```json
{
  "data_dir": "/var/lib/cfipd",
  "socket": "/run/cfipd/ctl.sock",
  "relay_listen": ":8443"
}
```

Add a field from the table below to change its default. After an edit, run
`cfipd test`. It reports a missing file, malformed JSON, unknown keys, and
out-of-range values, and it exits with status 1 on an error. The daemon stops
only for a file that it cannot read or parse. It starts with built-in defaults when the
file is missing and ignores unknown keys, so a stale key cannot stop a service.
It logs every problem that `cfipd test` reports.

| Field | Purpose | Default |
|---|---|---|
| `allowed_colos` | Accepted Cloudflare data center codes | `NRT`, `KIX`, `HKG`, `SIN` |
| `zone_probe_host` | A proxied (orange-cloud) hostname used to reject edge IPs that answer Cloudflare error 1034 for ordinary zones; empty disables the check | empty |
| `active_count` | Number of active endpoints | `3` |
| `relay_listen` | HTTPS relay bind address; empty string disables it | `:8443` |
| `web_password` | Password for the dashboard (HTTP Basic auth, any user name); empty disables it | empty |
| `health_interval` | Starting health-check interval | `"10s"` |
| `scan_v6_ratio` | Share of new-address discovery assigned to IPv6 | `0.2` |
| `promote_min_mbps` | Minimum throughput required for promotion | `30` |
| `bandwidth_weight` | Weight of bandwidth against probe time in the score, from `0` (probe time only) to `1` (bandwidth only) | `0.4` |
| `speed_recheck` | Interval between bandwidth rechecks of each active; `"0s"` disables them | `"12h"` |
| `speed_gap` | Minimum time between the starts of bandwidth tests; an HTTP 429 answer pauses tests for at least an hour | `"15m"` |
| `memory_limit_mb` | Go soft memory limit in MiB; `0` follows the host: current use plus a quarter of `MemAvailable`, with at least 4 MiB or a quarter of current use as headroom, and never under 8 MiB | `0` |

The full configuration is defined in
[config.go](https://github.com/breakertt/cfipd/blob/main/cmd/cfipd/config.go). A direct run
uses `/tmp/cfipd.sock` unless overridden; the Debian package supplies the
`/run/cfipd/ctl.sock` setting. Direct runs need write access to their data and
socket locations. If changing those paths for a packaged service, also adjust its
systemd directory settings and permissions.

## Command-line interface

The installed package's control commands require `sudo`. They communicate with
the daemon through its Unix socket.

| Command | Purpose |
|---|---|
| `cfipd status` | Pool sizes, health, scheduling, and PK status |
| `cfipd actives` / `cfipd standbys` / `cfipd candidates` | Inspect endpoint pools |
| `cfipd prefixes` | Inspect learned prefix statistics |
| `cfipd relay` | Per-endpoint connection and traffic statistics |
| `cfipd clients` | Relay connections and traffic per client IP since start (at most 256 clients) |
| `cfipd conns` | Each open relay connection and the latest 100 closed ones: client, SNI, upstream, duration, traffic |
| `cfipd scan` | Schedule a discovery probe |
| `cfipd import FILE` | Import IPs or a CloudflareSpeedTest CSV as candidate seeds |
| `cfipd promote IP` | Qualify a standby for a vacant active slot |
| `cfipd quarantine IP` | Cool an address down before revalidation |
| `cfipd test` | Check the configuration file; exit status 1 on an error |
| `cfipd version` | Show version, source commit, and build time |

The dashboard can also be started directly with `cfipd web`. It listens on
`127.0.0.1:8080` by default; `-listen` changes the bind address. Its read-only APIs
are `/api/status`, `/api/standbys`, `/api/relay`, `/api/clients`, and `/api/conns`. The page requests them with
relative paths, so a reverse proxy can mount the dashboard under a subpath such as
`/cfipd/`. The proxy must strip the prefix, and the page URL must end with `/`.

## How it works

Addresses move through three roles:

| Role | Purpose | Default capacity |
|---|---|---|
| Candidate | Short-lived validation of useful discoveries | Up to 256; two per /24 or /48 |
| Standby | Qualified backups awaiting promotion or comparison | Up to 8 |
| Active | Endpoints selected for new relayed HTTPS connections | 3 |

Discovery samples bundled Cloudflare ranges and learns from prefix-level probe
results. Failed or unwanted-region discoveries inform that learning without
creating permanent retry work. Candidate validation and scanning follow current
backup needs and improvement opportunities.

Active checks back off from 10 to 30, 60, and 120 seconds as availability
stabilizes, and accelerate on anomalies. Two preferred standbys are checked about
every five minutes; other backups about every thirty minutes. Slow endpoints
that still work continue serving until a replacement qualifies.

Fill uses a backup from a different colo when one is available
(`max_active_per_colo`, default 2). If no such backup can be tried, fill uses a
backup from the same colo. The limit of one active per /24 always applies. A
standby that fails a promotion test waits 10 minutes before its next test, then
20, 40 and so on, up to a day.

With default settings, elective replacement uses a fresh paired comparison (PK):
the challenger must pass every availability check and improve median HTTPS probe
time by at least 20% and 30 ms without worsening p90. A failed incumbent probe
counts as slower than any success, so an incumbent that fails half of its probes
loses on availability alone. A second comparison of the same
pair, at least five minutes later, confirms the result. A bandwidth comparison
then downloads from both endpoints. The challenger must reach 30 Mbps, unless the
incumbent itself measures under 30 Mbps, and it must have the higher score. Full
bandwidth comparisons are limited to two per day. A pair that loses the
bandwidth comparison is not compared again for a day; if a download fails, the
pair waits an hour. During a new state's
first 24 hours, PK records proposed swaps without applying them. Fault recovery
still operates, and the observation deadline survives restarts. PK also runs
when the pool has fewer actives than `active_count` and no backup can fill it.

The score combines probe time and bandwidth: probe time^−(1−w) × bandwidth^w, where
w is `bandwidth_weight`. The score uses ratios, so the units cancel. With the
default w of 0.4, 1% less probe time counts 1.5 times as much as 1% more bandwidth.
With w = 0, only probe time counts. With w = 1, only bandwidth counts. Floors come
before the score, so neither side can drop too far for the other. In PK, the
challenger is always faster and is held to the bandwidth floor above, so the
score never trades probe time away. With the default w, a challenger that is 20%
faster needs about 72% of the incumbent's bandwidth. In the relay weights, an active over 1.3
times the probe time of the best active keeps a quarter of its weight.

Each active gets a new bandwidth measurement about every 12 hours
(`speed_recheck`), with random jitter. A first result under 30 Mbps starts a
confirmation measurement about an hour later. After two such results in a row,
and while another active measures at least 30 Mbps, the active continues to
serve with a quarter of its normal relay weight. Each low result from then on
gives the best standby that fits one full promotion test. If it passes, the two
endpoints swap roles. While another active reaches 30 Mbps, the relay weights
count a sample under 30 Mbps as 30 Mbps, so only a confirmed result cuts more.
Where no active reaches 30 Mbps, low results start no replacement, and the relay
weights use the measured bandwidth.

Bandwidth tests run one at a time and start at least 15 minutes apart
(`speed_gap`) to stay under Cloudflare rate limits. A PK test downloads twice in
a row, once for each endpoint. An HTTP 429 answer pauses all tests for at least
an hour and does not count against the standby. When fewer than half of
`active_count` actives remain, fill and a manual promote do not wait for the
gap; the 429 pause still applies. Otherwise the default gap allows at most 96 tests a day.

The implementation uses TCP passthrough to carry HTTPS. For each new client
connection, it selects an Active IP at random, with weights in proportion to the
score, and opens a
connection to that Cloudflare edge (port 443 by default), then copies encrypted
TLS bytes in both directions. TLS handshakes and HTTP requests are handled by the client
and Cloudflare. For the connection list, cfipd reads the server name (SNI) from the
plaintext TLS ClientHello as it passes; with Encrypted Client Hello (ECH), this is
the public outer name. Requests that reuse the same HTTPS connection keep the same
upstream; selection happens per connection, not per HTTP request. Swaps affect
new HTTPS connections; established sessions keep their existing upstream.

## Data and operational limits

- `state.json` stores endpoint pools, prefix feedback, schedules, and PK budgets.
  v4 prefix statistics are limited to the bundled /24 table. v6 statistics that
  the sampler no longer uses (not probed for 7 days) are dropped.
  `history.jsonl` records probes and events; segments rotate at approximately
  8 MiB and only the newest three archives are kept, so history stays under
  about 32 MiB. Each pruned archive is logged.
- Measurements describe the daemon machine's route to the configured probe
  hostname. After changing networks, preserve the old state and start fresh
  learning. There is no city or ISP-specific binding.
- Dashboard timing is HTTPS request duration, not pure TCP RTT. Displayed
  bandwidth is the most recent single-connection download test (promotion, PK,
  or recheck), not live throughput.
  The default download test is capped at 99,999,999 bytes and ten seconds.
- The relay carries HTTPS over TCP; HTTP/3 over QUIC/UDP is outside its scope.
  It retries other active endpoints when an upstream
  connection attempt fails; it cannot rescue an established stream whose upstream
  dies. With no client traffic, a silent failure may take about two minutes to
  detect at the slowest default check interval.
- Pool and prefix data take about 1 MB. Most of the daemon's resident memory is
  the mapped binary (about 7 MB, shared with `cfipd web`) and Go runtime
  headroom. By default the soft memory limit follows the host's free memory, so
  it tightens only when the host runs short. On a small router, set
  `memory_limit_mb` (for example `12`) to keep the runtime tighter at the cost of
  more frequent garbage collection; the `GOMEMLIMIT` environment variable
  overrides it. `cfipd web` is a separate process, so leave it off where memory
  is tight. `cfipd status` shows the Go memory in use and the limit.
- The relay has no authentication. The dashboard has only the optional
  `web_password`, sent in clear text over plain HTTP. Restrict their bind addresses
  or network access. Keep the dashboard on loopback or a trusted network. If you
  set `web_password`, make `/etc/cfipd.json` readable only by root and `cfipd`. For
  the Debian package: `sudo chown root:cfipd /etc/cfipd.json && sudo chmod 640 /etc/cfipd.json`.

## Development

Use the Go version required by `go.mod` (currently Go 1.27 or newer). The project
uses the Go standard library without external runtime dependencies.

```bash
go build -o cfipd ./cmd/cfipd
go vet ./...
go test -race -count=1 ./...
```

On Linux, `bash scripts/release.sh VERSION dist` builds all release targets and
Debian packages. Package lifecycle tests in `scripts/test-package.sh` install and
remove services and must run on a disposable systemd host.

CI validates code, cross-builds binaries, and exercises package installation.
`v*` tags publish GitHub Releases after validation; branch and manual runs produce
CI artifacts. See [release notes](https://github.com/breakertt/cfipd/releases) for
version history and [AGENTS.md](https://github.com/breakertt/cfipd/blob/main/AGENTS.md)
for contributor conventions.
