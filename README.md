# zcode-cron-driver

Standalone headless scheduler for ZCode automations. Upstream ZCode only
dispatches scheduled automations from the Electron desktop app; on servers
running `zcode --web`, automations are written to the task index but never
fire. This driver makes them fire — dispatching turns through the harness
server the same way the desktop host does.

Zero npm dependencies: `node:sqlite`, `node:http`, `node:child_process`-free.
Requires Node ≥ 22.5 and a running harness server.

## How it works

Every 20 s (matching the desktop scheduler's cadence) it runs one cycle
against the harness's own `~/.zcode/v2/tasks-index.sqlite`:

1. **Claim** — one SQLite transaction expires past-`end_at` automations,
   reclaims zombie claims (>10 min), and atomically claims due rows
   (`running` 0→1). This is the upstream single-flight protocol, so the
   driver can never double-fire with the harness or a desktop app.
2. **Misfire** — windows missed by >5 min (first attempt only) are skipped:
   recurring tasks advance to the next occurrence, one-shots finalize.
   No catch-up bursts.
3. **Dispatch** — connects to the harness server as a *trusted host*
   (`POST /api/rpc-host-capability` for a one-time ticket, then the `/ws/host`
   websocket upgrade with the capability header — the desktop host's own
   connection class), then `resumeSession` → `setMode` → `sendPrompt` with
   the runId as `inputId` and the automation id as turn attribution.
   Includes a guard that defers dispatch while the target session is busy.
4. **Settle** — success: `run_count`+1, `next_run_at` recomputed from the
   cron/schedule rule, one-shot/`max_runs`/`end_at` transitions. Failure:
   retry with 30 s→15 min exponential backoff, 5 attempts, then recurring
   tasks skip to the next slot / one-shots go terminal `failed`. Turn
   completion is observed by polling `readSession`'s `runtime.activeTurnId`.

Scheduling semantics (claim/settle state machine, retry backoff, misfire
skip, one-shot finalization, run ledger, cron + scheduleRule math) are
faithful ports of upstream v3.14.3 (Apache-2.0):
`packages/services/src/session/automationRepo.ts`, `automationCron.ts`,
and the desktop scheduler.

## Usage

```bash
# peek at what would fire, running nothing:
node zcode-cron-driver.mjs --once --dry-run

# continuous daemon (singleton-locked, safe to restart any time):
node zcode-cron-driver.mjs

# self-healing via cron — starts every minute, flock makes extras no-ops:
* * * * * flock -n ~/.local/state/zcron.lock node /path/to/zcode-cron-driver.mjs >> ~/.local/state/zcron.log 2>&1
```

| Flag | Meaning |
| --- | --- |
| `--workspace <path>` | only dispatch automations in that workspace |
| `--once` | single cycle, wait for dispatches, exit |
| `--dry-run` | claim → log → release; never dispatches |
| `--db <path>` | task index (default `~/.zcode/v2/tasks-index.sqlite`) |
| `--server-url <url>` | default `ws://127.0.0.1:3030/ws` |
| `--token <t>` | default discovered from `ZCODE_SERVER_AUTH_TOKEN` or the server process env |
| `--timeout-ms <n>` | per-dispatch timeout, default 30 min |

Automations are created as usual from any zcode session (`CronCreate`) and
run in the session that created them, with full history.

## Tests

```bash
node zcode-cron-driver.test.mjs   # 43 tests, offline, safe to run anytime
```

## Known upstream issues worked around

- **Server crash via event listen**: `ChannelServer.onEventListen` does not
  catch `ProxyChannel`'s "Event not found" throw — any websocket client
  listening for an unexposed event kills the whole server process. The
  driver therefore never uses channel `listen()` and settles by polling.
- **Event read/write schema mismatch**: the server stores event payloads
  (e.g. `executionStartedAt`, `automationId`, `readOnly`) that its own
  `readSessionEvents` zod schema rejects, poisoning event pages.
  Settlement uses `readSession` snapshots instead.

Semantic trade-off: a *failed* turn (model error after acceptance) settles
as dispatched; connect/send failures still retry with full backoff.

## Requirements

- Node ≥ 22.5 (`node:sqlite`)
- A running ZCode harness server (the driver connects to it; if it's down,
  dispatches fail transient and retry — windows missed >5 min are skipped)
