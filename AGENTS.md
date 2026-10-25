# AGENTS.md

These are the project rules for code agents in the cfipd repository. The current instructions of
the user take precedence. [README.md](README.md) describes the use and the design.
`cmd/cfipd/config.go` is the reference for default values, and `scripts/release.sh` is the
reference for the build matrix. When you change behavior, update the related documents in the same
change. Do not copy the full parameter table into this file.

Write everything in the repository in English: code, comments, logs, error messages, scripts,
commit messages, and documents. There are two exceptions:

- `README.zh.md`, the Chinese copy of `README.md`.
- The `I18N.zh` table in `web.html`. The dashboard is bilingual. English is the source language.
  The browser language selects the language, and the button at the top right switches it and
  remembers the choice.

The second line of `README.md` links to `README.zh.md`. Keep the Chinese copy in the same
structure and with the same meaning. The order is: introduction, Quick Start, then configuration
and implementation details. The README is for users. Do not put handover notes, experiments, or
fix histories in it. The version history goes in the release notes, and work progress goes in the
progress notes outside the repository.

## Structure

cfipd is a Cloudflare HTTPS relay, an IP selection daemon, and a read-only web dashboard, written
with the Go standard library. Describe the HTTPS use to users. The implementation is an L4 TCP
passthrough: TLS runs end to end between the client and Cloudflare. The relay does not terminate
TLS and does not implement the HTTP CONNECT proxy protocol.

The Go source is a single main package in `cmd/cfipd/`. All source files in the table are in this
directory. A test file has the name of the file that it tests, `<source>_test.go`. Shared fixtures
are in `fixtures_test.go`.

| File | Function |
|---|---|
| `main.go` / `config.go` | CLI, version information, configuration lookup, loading, checking (`cfipd test`), and defaults |
| `daemon.go` | Single event loop, job scheduling, application of async results, and promotion |
| `pk.go` | Elective PK: pair selection, same-window comparison, independent confirmation, and bandwidth comparison |
| `bandwidth.go` | Periodic bandwidth rechecks of actives (two consecutive samples under the promotion floor confirm a low result), then the search for a replacement |
| `ip.go` | Roles, health state, real observation windows, candidate life cycle, diversity rules, and the latency and bandwidth `score` |
| `probe.go` / `scanner.go` | HTTPS probes, timed download tests, address sampling, and prefix feedback |
| `store.go` | Atomic JSON snapshots (written one entry at a time), migration of old formats, JSONL history, and rotation |
| `relay.go` | TCP passthrough with shared 16 KiB copy buffers (after a half-close, the connection closes when the other direction finishes or makes no progress for one minute), an atomic snapshot for new connections, real traffic feedback, a usage table per client IP (at most 256), and a log of connections (open ones plus the latest 100 closed, with the SNI from the plaintext ClientHello) |
| `memory.go` | Go soft memory limit, fixed or adjusted every minute from the free memory of the host. When the host has less free memory than twice the Go memory in use (`go_in_use`), it drops old prefix statistics and lowers the candidate cap |
| `ctl.go` / `web.go` / `web.html` | Unix socket control interface and a separate read-only dashboard (optional single-password Basic authentication) |
| `packaging/` / `scripts/` / `.github/workflows/ci.yml` | systemd, Debian package, build, and release acceptance |

## Implementation rules

- Keep the mutable pool and prefix state in the single event loop. A worker reads only the
  immutable input of its dispatch. When a result returns to the event loop, check the role and the
  generation. Check the generation of each IP that the result concerns, not a pool-wide generation,
  and check generation 0 too. The relay selects targets from an atomic snapshot, and the snapshot
  carries the generation of each active.
- Record each physical probe with its real time and its success or failure, and count it
  accurately. A role change must not fake observation times or drop failed samples. Keep client
  connection statistics apart from synthetic probe statistics. Do not label a total HTTP probe time
  as a pure TCP RTT.
- Run all bandwidth downloads (promotion, PK, recheck, replacement, manual promote) one at a time
  through `longJobBlocked`, and keep the `speed_gap` interval. Fill and manual promote do not wait
  for the interval when fewer than half of the actives remain, and an urgent PK waits only
  `urgentGap`. Always pause on HTTP 429. The
  decision that starts a PK also covers its bandwidth stage, because the PK holds `pkRunning` for
  the whole time. A new download path must use the same check.
- Drive scheduling by real demand, benefit, and the deadline of each IP. Capacities and budgets are
  limits, not quotas to fill. The prefix key, the eligibility criteria, and the architecture map
  each have one authoritative implementation. Do not copy their logic.
- A slow but usable active keeps serving. An active under the promotion floor also keeps serving
  until a standby passes a full promotion test and replaces it one for one. Cut the weight and try
  a replacement only when another active reaches the floor.
- Relay weights and the PK bandwidth stage compare only by `score` (rtt^-(1-w)·Mbps^w, with w =
  `bandwidth_weight`). The floors come before the score. The relay cuts the weight of an active
  over `rttLimit` or confirmed under the promotion floor. A PK challenger must be faster and reach
  the bandwidth floor, unless the incumbent itself is under the floor.
- An elective replacement needs a fresh same-window PK, an independent confirmation of the same
  pair, and a valid bandwidth comparison. PK compares the incumbent's `pkRef` with the challenger's
  `recentRTT` and takes the fastest eligible standby. While an active is degraded, standbys are
  probed every `urgentStandbyEvery`. An urgent pair (incumbent `pkRef` at least `urgentRatio`
  times the challenger) is the exception: it starts every `urgentPKEvery`, runs `urgentProbes`
  probes per side with `urgentExtraFails` losses allowed, and goes to the bandwidth comparison
  without the confirmation round or the daily budget. It keeps the score, the floors, and the
  generation checks. Before the commit, check roles, diversity rules, and generations again. A swap is one for one and does not change the number of actives. An atomic
  swap does not affect established relay connections.
- Snapshot migrations and restarts keep the real validation progress, the schedules, the PK
  observation deadline, and the daily counts and budgets. Log rotation keeps only the latest
  `histKeep` old segments and logs each older segment that it deletes. The documents state the
  total disk limit.
- The default colo selection is configurable. Do not add special cases for cities, ISPs, or
  machine addresses. The learning data describes only the egress of the probing machine. When the
  egress changes, keep the old data and learn again in a new data directory.
- Prefer the narrowest fix at the contract boundary, and keep the Go standard-library
  implementation. A new dependency or structure needs a concrete benefit. Reproduce a defect with a
  behavior test first, then fix it. Mark constructed test data clearly as a fixture. Never present
  it as a production measurement.

## Verification

Use a Go toolchain that matches `go.mod`. For Go changes, format only the files that you changed,
and run these commands for the scope of the change:

```bash
go vet ./...
go test -race -count=1 ./...
go build ./...
git diff --check
```

For document changes, check the content and run `git diff --check`. Do not run network probes
again for documents only. For packaging changes, build `bash scripts/release.sh VERSION dist` on
Linux, and check the ELF architecture, static linking, package metadata, and `SHA256SUMS`. A
successful cross-compile does not mean that the binary ran on that ISA.

`scripts/test-package.sh` installs, starts, stops, and removes the service. **Run it only on a
disposable systemd test machine.** It checks automatic start, the optional web service, that the
configuration and the administrator's disable state stay, and that the learning data stays after
removal. Its network isolation is not a real Cloudflare acceptance test. The state directory is
protected. Use `sudo` to inspect it, and do not loosen the service permissions to make the test
pass.

## Git, release, and deployment

- Check the working tree before you commit. Fetch and check the target branch before you push. Do
  not overwrite the changes of others, and do not rewrite existing tags. Use the configured Git
  identity. Ask the user before you set or change the identity or the author. Do not add agent
  attributions or Co-Authored-By lines.
- main, pull requests, and manual CI runs produce build artifacts. A new `v*` tag publishes a real
  GitHub Release only after it passes acceptance. To release, use a new version tag. Do not
  force-retag to repair a failed pipeline. Do not create a version for documentation changes only.
- Release acceptance checks the CI result, the real Release assets, the architecture, version, and
  source commit, and the checksums. Keep these kinds of evidence apart: compilation, install life
  cycle, connectivity, and real performance. One kind does not replace another.
- A built or published package is not permission to migrate a production service. Before a
  deployment, check the target change, the processes, connections, ports, and data ownership. Back
  up the binary, the configuration, and a consistent state, and keep a rollback path. Do not let
  two daemons share one data directory.
- The Debian package uses the dedicated `cfipd` user, the configuration `/etc/cfipd.json`, the
  state `/var/lib/cfipd`, and the socket `/run/cfipd/ctl.sock`. Keep the configuration, the
  administrator's disable state, and the learning data. By default, the web service binds only to
  loopback.
- Do not commit real configurations, learning state, history logs, build products, or credentials.
  Do not put tokens in command arguments or logs. Deletions and cleanups obey the user's rules for
  recoverability. Do not use forced overwrites or destructive cleanups as shortcuts.
- Run long jobs in tmux on the target machine. Check the session state before you send input.
  Report infrastructure failures first. Do not change the network, authentication, or service
  layout of a machine on the side.

Progress notes are in `~/.agents/projects/cfipd/PROGRESS.md`, and plans are in
`~/.agents/plans/cfipd/`. They are local working state. Do not commit them. If shared project
memory exists, read `~/.agents/memory/cfipd/MEMORY.md` and its index first.
