#!/usr/bin/env -S node --no-warnings
// zcode-cron-driver — standalone headless scheduler for ZCode automations.
//
// Why this exists: upstream ZCode only dispatches scheduled automations from the
// Electron desktop app (packages/desktop/src/scheduler/index.ts, forked by desktop
// main via electronUtilityProcess.fork). On headless boxes running `zcode --web`,
// automations are written to tasks-index.sqlite but never dispatched.
//
// This driver polls the same database with the same claim/settle state machine,
// so it stays compatible with the desktop scheduler if one ever runs alongside
// (single-flight claims make double dispatch impossible).
//
// Scheduling semantics (claim protocol, retry backoff, misfire skip, one-shot
// finalization, run ledger) are ported from the Apache-2.0 licensed upstream:
// packages/services/src/session/automationRepo.ts, automationCron.ts and
// packages/desktop/src/scheduler/index.ts @ v3.14.3.
//
// Zero npm dependencies: node:sqlite + child_process only. Run `node --version`
// needs >= 22.5. Standalone by design — survives harness updates unless the
// database schema itself changes (guarded at startup: it refuses loudly, never
// corrupts).
//
// Usage:
//   zcode-cron-driver.mjs                 # continuous, poll every 20s
//   zcode-cron-driver.mjs --once          # single tick, wait for dispatches, exit
//   Options:
//     --db <path>          tasks-index.sqlite (default ~/.zcode/v2/tasks-index.sqlite)
//     --workspace <path>   only dispatch automations for this workspace
//     --timeout-ms <n>     per-dispatch timeout (default 30 min)
//     --interval-ms <n>    poll interval in continuous mode (default 20000)
//     --lock-file <path>   singleton lock (default ~/.local/state/zcode-cron-driver.lock)
//     --dry-run            claim, log, release — no dispatch, no state changes
//     --server-url <url>   harness server ws endpoint (default ws://127.0.0.1:3030/ws)
//     --token <token>      server auth token; when given, auto-discovery is off
//
// Dispatch executes: zcode [--resume <bound session>] --cwd <workspace>
//                      [--mode <mode>] -p "<prompt>"
import { DatabaseSync } from "node:sqlite";
import http from "node:http";
import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, openSync, readFileSync, readdirSync, readlinkSync, unlinkSync, writeSync, closeSync } from "node:fs";
import { homedir } from "node:os";
import { join, dirname } from "node:path";
import { pathToFileURL } from "node:url";

// ---- constants ported from upstream ----
export const POLL_INTERVAL_MS = 20_000;
export const MISFIRE_GRACE_MS = 5 * 60_000; // desktop scheduler: misfire grace
export const CLAIM_STALE_MS = 10 * 60_000; // automationRepo: zombie claim reclaim
export const DISPATCH_MAX_ATTEMPTS = 5; // automationRepo: give-up threshold
export const DISPATCH_RETRY_BASE_MS = 30_000;
export const DISPATCH_RETRY_CAP_MS = 15 * 60_000;
export const DEFAULT_DISPATCH_TIMEOUT_MS = 30 * 60_000;

const MIN = 60_000;

function log(level, message) {
  const line = `${new Date().toISOString()} [${level}] ${message}`;
  if (level === "error") console.error(line);
  else console.log(line);
}

// ---- cron next-run (5-field, local time, vixie dom/dow OR semantics) ----

function expandPiece(piece, min, max) {
  // piece: "*" | "a" | "a-b" | "a-b/s" | "*/s"  → array of ints, or null for wildcard
  let step = 1;
  const slash = piece.indexOf("/");
  if (slash >= 0) {
    step = Number(piece.slice(slash + 1));
    piece = piece.slice(0, slash);
    if (!Number.isInteger(step) || step < 1) return undefined; // parse failure
  }
  let lo;
  let hi;
  if (piece === "*" || piece === "") {
    lo = min;
    hi = max;
    if (slash < 0) return null; // plain wildcard
  } else if (piece.includes("-")) {
    const [l, h] = piece.split("-");
    lo = Number(l);
    hi = Number(h);
    if (!Number.isInteger(lo) || !Number.isInteger(hi)) return undefined;
  } else {
    const v = Number(piece);
    if (!Number.isInteger(v)) return undefined;
    lo = v;
    hi = slash >= 0 ? v : v; // "5/2" == just 5; "5" == just 5
    if (slash < 0) hi = v;
  }
  const out = [];
  for (let v = lo; v <= hi; v += step) {
    if (v >= min && v <= max) out.push(v);
  }
  return out;
}

function parseField(token, min, max, map) {
  if (token === "?" ) token = "*";
  const values = new Set();
  let wildcard = false;
  for (const piece of token.split(",")) {
    const r = expandPiece(piece, min, max);
    if (r === undefined) return undefined; // unparseable → caller deems expr invalid
    if (r === null) {
      wildcard = true;
    } else {
      for (const v of r) values.add(map ? map(v) : v);
    }
  }
  return { values, wildcard };
}

const CRON_HORIZON_MS = 5 * 366 * 24 * 60 * MIN; // bail after ~5 years

/** Next match strictly after `from` (ms epoch), minute resolution, local time. */
export function nextCronRun(cronExpr, from) {
  if (typeof cronExpr !== "string") return null;
  const parts = cronExpr.trim().split(/\s+/);
  if (parts.length !== 5) return null;
  const minute = parseField(parts[0], 0, 59);
  const hour = parseField(parts[1], 0, 23);
  const dom = parseField(parts[2], 1, 31);
  const month = parseField(parts[3], 1, 12);
  // dow: 0-7, with 7 aliasing Sunday(0)
  const dow = parseField(parts[4], 0, 7, (v) => (v === 7 ? 0 : v));
  if ([minute, hour, dom, month, dow].some((f) => f === undefined)) return null;

  const domRestricted = !dom.wildcard;
  const dowRestricted = !dow.wildcard;

  let t = Math.floor(from / MIN) * MIN + MIN;
  const horizon = from + CRON_HORIZON_MS;
  while (t <= horizon) {
    const d = new Date(t);
    if (
      (minute.wildcard || minute.values.has(d.getMinutes())) &&
      (hour.wildcard || hour.values.has(d.getHours())) &&
      (month.wildcard || month.values.has(d.getMonth() + 1))
    ) {
      const domOk = dom.wildcard || dom.values.has(d.getDate());
      const dowOk = dow.wildcard || dow.values.has(d.getDay());
      const dayOk =
        domRestricted && dowRestricted ? domOk || dowOk : domOk && dowOk;
      if (dayOk) return t;
    }
    t += MIN;
  }
  return null;
}

// ---- scheduleRule next-run (port of computeScheduleRuleNextRunAt) ----

function atTime(date, hour, minute) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), hour, minute, 0, 0);
}

function firstWeekdayOfMonth(year, month, weekday) {
  const first = new Date(year, month, 1);
  return new Date(year, month, 1 + ((weekday - first.getDay() + 7) % 7));
}

export function computeScheduleRuleNextRunAt(rule, from) {
  const interval = Math.max(1, Math.floor(rule.interval));
  const anchor = new Date(rule.anchorAt);

  if (rule.unit === "minute") {
    const step = interval * MIN;
    const steps = Math.max(1, Math.floor((from - rule.anchorAt) / step) + 1);
    return rule.anchorAt + steps * step;
  }

  if (rule.unit === "hourly") {
    const base = new Date(anchor);
    base.setMinutes(rule.minute, 0, 0);
    const step = interval * 60 * MIN;
    const steps = Math.max(0, Math.floor((from - base.getTime()) / step) + 1);
    return base.getTime() + steps * step;
  }

  if (rule.unit === "daily") {
    for (let index = 0; index < 36_600; index += 1) {
      const date = new Date(
        anchor.getFullYear(),
        anchor.getMonth(),
        anchor.getDate() + index * interval,
      );
      const candidate = atTime(date, rule.hour, rule.minute).getTime();
      if (candidate > from) return candidate;
    }
    return null;
  }

  if (rule.unit === "weekly") {
    const anchorWeek = new Date(anchor.getFullYear(), anchor.getMonth(), anchor.getDate());
    anchorWeek.setDate(anchorWeek.getDate() - ((anchorWeek.getDay() + 6) % 7));
    const weekdays = [...(rule.weekdays?.length ? rule.weekdays : [1])].sort((a, b) => a - b);
    for (let week = 0; week < 5_220; week += interval) {
      for (const weekday of weekdays) {
        const dayOffset = (weekday + 6) % 7;
        const date = new Date(
          anchorWeek.getFullYear(),
          anchorWeek.getMonth(),
          anchorWeek.getDate() + week * 7 + dayOffset,
        );
        const candidate = atTime(date, rule.hour, rule.minute).getTime();
        if (candidate > from) return candidate;
      }
    }
    return null;
  }

  if (rule.unit === "monthly") {
    for (let offset = 0; offset <= 1_200; offset += interval) {
      const month = new Date(anchor.getFullYear(), anchor.getMonth() + offset, 1);
      const candidates =
        rule.monthlyMode === "weekday"
          ? [firstWeekdayOfMonth(month.getFullYear(), month.getMonth(), rule.weekdays?.[0] ?? 1)]
          : [...(rule.monthDays?.length ? rule.monthDays : [1])]
              .sort((left, right) => left - right)
              .map((day) => new Date(month.getFullYear(), month.getMonth(), day))
              .filter((date) => date.getMonth() === month.getMonth());
      for (const date of candidates) {
        const candidate = atTime(date, rule.hour, rule.minute).getTime();
        if (candidate > from) return candidate;
      }
    }
    return null;
  }

  // yearly
  const targetMonth =
    rule.months?.[0] != null ? (((rule.months[0] - 1) % 12) + 12) % 12 : anchor.getMonth();
  const targetDay = rule.monthDays?.[0] ?? anchor.getDate();
  for (let offset = 0; offset < 400; offset += interval) {
    const date = new Date(anchor.getFullYear() + offset, targetMonth, targetDay);
    if (date.getMonth() !== targetMonth) continue;
    const candidate = atTime(date, rule.hour, rule.minute).getTime();
    if (candidate > from) return candidate;
  }
  return null;
}

export function computeAutomationNextRunAt(automation, from) {
  return automation.scheduleRule
    ? computeScheduleRuleNextRunAt(automation.scheduleRule, from)
    : nextCronRun(automation.cronExpr, from);
}

/** 纯一次性任务（port of isOneShotAutomation）: non-recurring and runs at most once. */
export function isOneShot(automation) {
  return !automation.recurring && (automation.maxRuns ?? 1) <= 1;
}

/** 退避重试时间（port of computeRetryAt）: now + min(BASE * 2^(attempts-1), CAP). */
export function computeRetryAt(now, attempts) {
  const backoff = Math.min(
    DISPATCH_RETRY_BASE_MS * 2 ** Math.max(0, attempts - 1),
    DISPATCH_RETRY_CAP_MS,
  );
  return now + backoff;
}

/** 'misfire' when the window was missed beyond grace on a first attempt, else 'dispatch'. */
export function classifyClaim(automation, now) {
  const isRetry = automation.dispatchAttempts > 0;
  const missed =
    !isRetry && automation.nextRunAt != null && automation.nextRunAt <= now - MISFIRE_GRACE_MS;
  return missed ? "misfire" : "dispatch";
}

// ---- row <-> domain mapping ----

function parseScheduleRule(raw) {
  if (!raw) return null;
  try {
    const rule = JSON.parse(raw);
    return rule && typeof rule === "object" && typeof rule.unit === "string" ? rule : null;
  } catch {
    return null;
  }
}

function rowToAutomation(row) {
  return {
    automationId: row.automation_id,
    title: row.title,
    cronExpr: row.cron_expr,
    prompt: row.prompt,
    mode: row.mode ?? null,
    workspaceKey: row.workspace_key,
    workspacePath: row.workspace_path,
    workspaceIdentity: row.workspace_identity ?? null,
    targetTaskId: row.target_task_id ?? null,
    locationKind: row.location_kind ?? "local",
    recurring: row.recurring,
    maxRuns: row.max_runs ?? null,
    endAt: row.end_at ?? null,
    scheduleRule: parseScheduleRule(row.schedule_rule),
    runCount: row.run_count,
    scheduledRunCount: row.scheduled_run_count,
    enabled: row.enabled,
    lifecycleStatus: row.lifecycle_status,
    nextRunAt: row.next_run_at ?? null,
    retryAt: row.retry_at ?? null,
    running: row.running,
    claimedAt: row.claimed_at ?? null,
    dispatchStatus: row.dispatch_status,
    dispatchAttempts: row.dispatch_attempts,
    lastError: row.last_error ?? null,
  };
}

// ---- schema ----
// Mirrors the live tasks-index automations/automation_runs tables (v3.14.3).
// Used only to build scratch databases for the test suite; against the real
// database the driver only ever runs guarded DML.

export const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS automations (
  automation_id TEXT PRIMARY KEY,
  title TEXT NOT NULL DEFAULT '',
  cron_expr TEXT NOT NULL,
  prompt TEXT NOT NULL,
  model TEXT,
  provider TEXT,
  mode TEXT,
  thought_level TEXT,
  model_selection TEXT,
  workspace_key TEXT NOT NULL,
  workspace_path TEXT NOT NULL,
  workspace_identity TEXT,
  target_task_id TEXT,
  bot_delivery_target TEXT,
  location_kind TEXT NOT NULL DEFAULT 'local',
  recurring INTEGER NOT NULL DEFAULT 1,
  max_runs INTEGER,
  end_at INTEGER,
  schedule_rule TEXT,
  schedule_edited_by_user INTEGER NOT NULL DEFAULT 0,
  run_count INTEGER NOT NULL DEFAULT 0,
  scheduled_run_count INTEGER NOT NULL DEFAULT 0,
  enabled INTEGER NOT NULL DEFAULT 1,
  lifecycle_status TEXT NOT NULL DEFAULT 'active',
  next_run_at INTEGER,
  last_run_at INTEGER,
  running INTEGER NOT NULL DEFAULT 0,
  claimed_at INTEGER,
  dispatch_status TEXT NOT NULL DEFAULT 'idle',
  dispatch_attempts INTEGER NOT NULL DEFAULT 0,
  retry_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS automation_runs (
  run_id TEXT PRIMARY KEY,
  automation_id TEXT NOT NULL,
  workspace_key TEXT,
  scheduled_at INTEGER,
  trigger TEXT NOT NULL DEFAULT 'schedule',
  model_selection TEXT,
  dispatch_status TEXT NOT NULL DEFAULT 'claimed',
  outcome TEXT,
  session_id TEXT,
  error TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
`;

const REQUIRED_AUTOMATION_COLUMNS = [
  "automation_id", "cron_expr", "prompt", "mode", "workspace_key", "workspace_path",
  "target_task_id", "location_kind", "recurring", "max_runs", "end_at", "schedule_rule",
  "run_count", "scheduled_run_count", "enabled", "lifecycle_status", "next_run_at",
  "last_run_at", "running", "claimed_at", "dispatch_status", "dispatch_attempts",
  "retry_at", "last_error", "updated_at",
];
const REQUIRED_RUN_COLUMNS = [
  "run_id", "automation_id", "workspace_key", "scheduled_at", "trigger",
  "model_selection", "dispatch_status", "session_id", "error", "attempts",
  "created_at", "updated_at",
];

// ---- store: the claim/settle state machine (port of AutomationRepo) ----

function withBusyRetry(fn, attempts = 10) {
  let lastError;
  for (let i = 0; i < attempts; i++) {
    try {
      return fn();
    } catch (error) {
      lastError = error;
      if (!String(error?.message ?? "").includes("SQLITE_BUSY")) throw error;
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
    }
  }
  throw lastError;
}

export class AutomationStore {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
  }

  /** Throws with a clear message if the live schema no longer matches expectations. */
  verifySchema() {
    for (const [table, required] of [
      ["automations", REQUIRED_AUTOMATION_COLUMNS],
      ["automation_runs", REQUIRED_RUN_COLUMNS],
    ]) {
      const cols = new Set(
        this.db.prepare(`PRAGMA table_info(${table})`).all().map((c) => c.name),
      );
      if (cols.size === 0) {
        throw new Error(`schema guard: table "${table}" is missing — not a tasks-index database?`);
      }
      const missing = required.filter((c) => !cols.has(c));
      if (missing.length > 0) {
        throw new Error(
          `schema guard: ${table} no longer has columns [${missing.join(", ")}] — ` +
            `the harness schema changed; this driver needs a review. Refusing to touch the database.`,
        );
      }
    }
  }

  static createSchema(db) {
    db.exec(SCHEMA_SQL);
  }

  getRow(automationId) {
    return this.db.prepare(`SELECT * FROM automations WHERE automation_id = ?`).get(automationId);
  }

  getRun(runId) {
    return this.db.prepare(`SELECT * FROM automation_runs WHERE run_id = ?`).get(runId);
  }

  /** Single-flight claim of due automations (port of claimDue). Returns raw rows. */
  claimDue(now, workspacePath) {
    return withBusyRetry(() => {
      this.db.exec("BEGIN IMMEDIATE");
      try {
        // expire finished windows without dispatching
        this.db
          .prepare(
            `UPDATE automations
             SET lifecycle_status = 'completed', enabled = 0, next_run_at = NULL,
                 retry_at = NULL, running = 0, claimed_at = NULL, updated_at = ?
             WHERE enabled = 1 AND end_at IS NOT NULL AND end_at < ?`,
          )
          .run(now, now);
        // reclaim zombie claims
        this.db
          .prepare(
            `UPDATE automations SET running = 0, claimed_at = NULL
             WHERE running = 1 AND claimed_at IS NOT NULL AND claimed_at <= ?`,
          )
          .run(now - CLAIM_STALE_MS);
        const dueRows = withBusyRetry(() =>
          workspacePath
            ? this.db
                .prepare(
                  `SELECT * FROM automations
                   WHERE enabled = 1 AND running = 0 AND workspace_path = ?
                     AND ((retry_at IS NOT NULL AND retry_at <= ?)
                       OR (retry_at IS NULL AND next_run_at IS NOT NULL AND next_run_at <= ?))`,
                )
                .all(workspacePath, now, now)
            : this.db
                .prepare(
                  `SELECT * FROM automations
                   WHERE enabled = 1 AND running = 0
                     AND ((retry_at IS NOT NULL AND retry_at <= ?)
                       OR (retry_at IS NULL AND next_run_at IS NOT NULL AND next_run_at <= ?))`,
                )
                .all(now, now),
        );
        const claim = this.db.prepare(
          `UPDATE automations
           SET running = 1, claimed_at = ?, dispatch_status = 'claimed', updated_at = ?
           WHERE automation_id = ? AND running = 0`,
        );
        const claimed = [];
        for (const row of dueRows) {
          const res = claim.run(now, now, row.automation_id);
          if (res.changes === 1) {
            claimed.push({
              ...row,
              running: 1,
              claimed_at: now,
              dispatch_status: "claimed",
            });
          }
        }
        this.db.exec("COMMIT");
        return claimed;
      } catch (error) {
        try {
          this.db.exec("ROLLBACK");
        } catch {
          // BEGIN itself failed (e.g. SQLITE_BUSY) — no transaction to roll back
        }
        throw error;
      }
    });
  }

  /** 派发成功结算（port of markDispatched）. */
  markDispatched(automationId, { dispatchedAt, nextRunAt }) {
    const row = this.getRow(automationId);
    if (!row) return; // deleted meanwhile — never resurrect
    const runCount = row.run_count + 1;
    const scheduledRunCount = row.scheduled_run_count + 1;
    const reachedMax = row.recurring === 0 && scheduledRunCount >= (row.max_runs ?? 1);
    const reachedEnd = row.end_at !== null && (nextRunAt ?? Infinity) > row.end_at;
    this.db
      .prepare(
        `UPDATE automations
         SET run_count = ?, scheduled_run_count = ?, last_run_at = ?,
             dispatch_status = 'dispatched', dispatch_attempts = 0, retry_at = NULL,
             last_error = NULL, running = 0, claimed_at = NULL,
             lifecycle_status = ?, enabled = ?, next_run_at = ?, updated_at = ?
         WHERE automation_id = ?`,
      )
      .run(
        runCount,
        scheduledRunCount,
        dispatchedAt,
        reachedMax || reachedEnd ? "completed" : "active",
        reachedMax || reachedEnd ? 0 : 1,
        reachedMax || reachedEnd ? null : nextRunAt,
        dispatchedAt,
        automationId,
      );
  }

  /** 派发失败结算（port of markDispatchFailed）. */
  markDispatchFailed(automationId, { failedAt, error, kind, nextRunAt }) {
    const row = this.getRow(automationId);
    if (!row) return;
    if (kind === "permanent") {
      this.db
        .prepare(
          `UPDATE automations
           SET dispatch_status = 'failed_to_dispatch', lifecycle_status = 'failed',
               enabled = 0, running = 0, claimed_at = NULL, last_error = ?, updated_at = ?
           WHERE automation_id = ?`,
        )
        .run(error, failedAt, automationId);
      return;
    }
    const attempts = row.dispatch_attempts + 1;
    if (attempts >= DISPATCH_MAX_ATTEMPTS) {
      if (row.recurring === 1) {
        this.db
          .prepare(
            `UPDATE automations
             SET dispatch_status = 'idle', dispatch_attempts = 0, retry_at = NULL,
                 running = 0, claimed_at = NULL, next_run_at = ?, last_error = ?, updated_at = ?
             WHERE automation_id = ?`,
          )
          .run(nextRunAt ?? null, error, failedAt, automationId);
      } else {
        this.db
          .prepare(
            `UPDATE automations
             SET dispatch_status = 'failed_to_dispatch', lifecycle_status = 'failed',
                 enabled = 0, running = 0, claimed_at = NULL, last_error = ?, updated_at = ?
             WHERE automation_id = ?`,
          )
          .run(error, failedAt, automationId);
      }
      return;
    }
    this.db
      .prepare(
        `UPDATE automations
         SET dispatch_status = 'failed_to_dispatch', dispatch_attempts = ?, retry_at = ?,
             running = 0, claimed_at = NULL, last_error = ?, updated_at = ?
         WHERE automation_id = ?`,
      )
      .run(attempts, computeRetryAt(failedAt, attempts), error, failedAt, automationId);
  }

  /** 关机/退出时释放认领（port of releaseClaim）. */
  releaseClaim(automationId) {
    this.db
      .prepare(
        `UPDATE automations
         SET running = 0, claimed_at = NULL, dispatch_status = 'idle', updated_at = ?
         WHERE automation_id = ? AND running = 1`,
      )
      .run(Date.now(), automationId);
  }

  /** 错过窗口的补偿跳过（port of skipAndReschedule）. */
  skipAndReschedule({ automationId, runId, workspaceKey, scheduledAt, reason, nextRunAt, finalize }) {
    const now = Date.now();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.db
        .prepare(
          `INSERT INTO automation_runs (
             run_id, automation_id, workspace_key, scheduled_at, trigger,
             dispatch_status, error, attempts, created_at, updated_at
           ) VALUES (?, ?, ?, ?, 'schedule', 'skipped', ?, 0, ?, ?)
           ON CONFLICT(run_id) DO UPDATE SET
             dispatch_status = 'skipped', error = excluded.error, updated_at = excluded.updated_at`,
        )
        .run(runId, automationId, workspaceKey, scheduledAt, reason, now, now);
      if (finalize) {
        this.db
          .prepare(
            `UPDATE automations
             SET lifecycle_status = 'completed', enabled = 0, next_run_at = NULL,
                 running = 0, claimed_at = NULL,
                 dispatch_status = 'idle', dispatch_attempts = 0, retry_at = NULL, updated_at = ?
             WHERE automation_id = ?`,
          )
          .run(now, automationId);
      } else {
        this.db
          .prepare(
            `UPDATE automations
             SET next_run_at = ?, running = 0, claimed_at = NULL,
                 dispatch_status = 'idle', dispatch_attempts = 0, retry_at = NULL, updated_at = ?
             WHERE automation_id = ?`,
          )
          .run(nextRunAt, now, automationId);
      }
      this.db.exec("COMMIT");
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {}
      throw error;
    }
  }

  /** 认领时 upsert run 台账（port of upsertRunClaimed）. */
  upsertRunClaimed({ runId, automationId, workspaceKey, scheduledAt, trigger }) {
    const now = Date.now();
    this.db
      .prepare(
        `INSERT INTO automation_runs (
           run_id, automation_id, workspace_key, scheduled_at, trigger,
           model_selection, dispatch_status, attempts, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, NULL, 'claimed', 0, ?, ?)
         ON CONFLICT(run_id) DO UPDATE SET
           dispatch_status = 'claimed',
           model_selection = COALESCE(automation_runs.model_selection, excluded.model_selection),
           outcome = NULL, error = NULL, attempts = attempts + 1,
           updated_at = excluded.updated_at`,
      )
      .run(runId, automationId, workspaceKey, scheduledAt, trigger, now, now);
  }

  /** 派发结果回写 run（port of markRunDispatch）. */
  markRunDispatch({ runId, dispatchStatus, sessionId, error }) {
    this.db
      .prepare(
        `UPDATE automation_runs
         SET dispatch_status = ?, session_id = COALESCE(?, session_id), error = ?, updated_at = ?
         WHERE run_id = ?`,
      )
      .run(dispatchStatus, sessionId ?? null, error ?? null, Date.now(), runId);
  }

  close() {
    this.db.close();
  }
}

// ---- scheduler orchestration (port of desktop scheduler tick/handleClaimed) ----

export class Scheduler {
  constructor({ store, dispatch, now = () => Date.now(), log: logger = log, dryRun = false, workspace = null }) {
    this.store = store;
    this.dispatch = dispatch;
    this.now = now;
    this.log = logger;
    this.dryRun = dryRun;
    this.workspace = workspace;
    this.inFlightIds = new Set();
  }

  async tick() {
    const now = this.now();
    const claimedRows = this.store.claimDue(now, this.workspace);
    for (const row of claimedRows) {
      await this.handleClaimed(rowToAutomation(row), now);
    }
    return claimedRows.length;
  }

  async handleClaimed(automation, now) {
    const scheduledAt = automation.nextRunAt ?? automation.retryAt ?? now;
    const runId = `${automation.automationId}:${scheduledAt}`;

    if (classifyClaim(automation, now) === "misfire") {
      const finalize = isOneShot(automation);
      const nextRunAt = finalize ? null : computeAutomationNextRunAt(automation, now);
      if (this.dryRun) {
        this.log("info", `dry-run: would skip missed window ${automation.automationId} (finalize=${finalize})`);
        this.store.releaseClaim(automation.automationId);
        return;
      }
      this.store.skipAndReschedule({
        automationId: automation.automationId,
        runId,
        workspaceKey: automation.workspaceKey,
        scheduledAt,
        reason: "computer_asleep_or_app_not_running",
        nextRunAt,
        finalize,
      });
      this.log(
        "info",
        `skip missed window automation=${automation.automationId} scheduledAt=${scheduledAt}` +
          `${finalize ? " finalized=one-shot" : ""}`,
      );
      return;
    }

    if (this.dryRun) {
      this.log(
        "info",
        `dry-run: would dispatch ${automation.automationId} ` +
          `(session=${automation.targetTaskId} prompt=${JSON.stringify(automation.prompt.slice(0, 80))}…)`,
      );
      this.store.releaseClaim(automation.automationId);
      return;
    }

    this.store.upsertRunClaimed({
      runId,
      automationId: automation.automationId,
      workspaceKey: automation.workspaceKey,
      scheduledAt,
      trigger: "schedule",
    });
    this.inFlightIds.add(automation.automationId);
    let result;
    try {
      result = await this.dispatch(automation);
    } catch (error) {
      result = { ok: false, error: error instanceof Error ? error.message : String(error) };
    } finally {
      this.inFlightIds.delete(automation.automationId);
    }
    await this.settle(automation, runId, result);
  }

  async settle(automation, runId, result) {
    const now = this.now();
    if (result.ok) {
      this.store.markRunDispatch({
        runId,
        dispatchStatus: "dispatched",
        sessionId: result.sessionId ?? automation.targetTaskId,
      });
      const nextRunAt = computeAutomationNextRunAt(automation, now);
      this.store.markDispatched(automation.automationId, { dispatchedAt: now, nextRunAt });
      this.log("info", `dispatched automation=${automation.automationId} runId=${runId} nextRunAt=${nextRunAt}`);
      return;
    }
    this.store.markRunDispatch({
      runId,
      dispatchStatus: "failed_to_dispatch",
      error: result.error ?? "dispatch failed",
    });
    const nextRunAt = computeAutomationNextRunAt(automation, now);
    this.store.markDispatchFailed(automation.automationId, {
      failedAt: now,
      error: result.error ?? "dispatch failed",
      kind: result.failureKind ?? "transient",
      nextRunAt,
    });
    this.log(
      "warn",
      `dispatch failed automation=${automation.automationId} runId=${runId} error=${result.error ?? "?"} next attempt follows backoff`,
    );
  }
}

// ---- desktop-style dispatch: zcode app-server protocol ----
// Wire format ported from upstream @zcode/rpc (packages/rpc/src): SocketProtocol
// frames [type u8][id u32][ack u32][len u32][body] carrying two tagged values
// (header, body); ChannelClient/ChannelServer semantics (Initialize=200,
// Promise=100, EventListen=102, PromiseSuccess=201, PromiseError=202,
// EventFire=204); ProxyChannel maps call(command, args[]) → service method and
// listen(event, arg) → on* / onDynamic*(arg) event.

export const FRAME_HEADER_SIZE = 13;
const PROTOCOL_FRAME_TYPE_REGULAR = 1;
const REQUEST = { PROMISE: 100, PROMISE_CANCEL: 101, EVENT_LISTEN: 102, EVENT_DISPOSE: 103 };
const RESPONSE = { INITIALIZE: 200, PROMISE_SUCCESS: 201, PROMISE_ERROR: 202, PROMISE_ERROR_OBJ: 203, EVENT_FIRE: 204 };
const DATA_TYPE = { UNDEFINED: 0, STRING: 1, BUFFER: 2, VSBUFFER: 3, ARRAY: 4, OBJECT: 5, INT: 6 };
export const AGENT_CHANNEL = "zcode-agent";

export function serializeRpcValue(writer, value) {
  if (value === undefined) {
    writer.push(DATA_TYPE.UNDEFINED);
  } else if (typeof value === "string") {
    const bytes = Buffer.from(value, "utf8");
    writer.push(DATA_TYPE.STRING, ...writeVql(bytes.length), ...bytes);
  } else if (value instanceof Uint8Array) {
    writer.push(DATA_TYPE.BUFFER, ...writeVql(value.length), ...value);
  } else if (Array.isArray(value)) {
    writer.push(DATA_TYPE.ARRAY, ...writeVql(value.length));
    for (const el of value) serializeRpcValue(writer, el);
  } else if (typeof value === "number" && Number.isInteger(value)) {
    writer.push(DATA_TYPE.INT, ...writeVql(value));
  } else {
    const bytes = Buffer.from(JSON.stringify(value), "utf8");
    writer.push(DATA_TYPE.OBJECT, ...writeVql(bytes.length), ...bytes);
  }
}

function writeVql(value) {
  // little-endian 7-bit groups, continuation bit set on all but the last byte
  if (value === 0) return [0];
  const out = [];
  for (let v = value; v !== 0; v = v >>> 7) {
    let b = v & 0b0111_1111;
    if (v >>> 7 !== 0) b |= 0b1000_0000;
    out.push(b);
  }
  return out;
}

export function deserializeRpcValue(bytes, offset = 0) {
  const u8 = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let pos = offset;
  const tag = u8[pos++];
  if (tag === DATA_TYPE.UNDEFINED) return { value: undefined, bytesConsumed: 1 };
  let shift = 0;
  let len = 0;
  for (;;) {
    const b = u8[pos++];
    len |= (b & 0b0111_1111) << shift;
    if (!(b & 0b1000_0000)) break;
    shift += 7;
  }
  const headerSize = pos - offset;
  switch (tag) {
    case DATA_TYPE.UNDEFINED:
      return { value: undefined, bytesConsumed: headerSize };
    case DATA_TYPE.INT:
      return { value: len, bytesConsumed: headerSize };
    case DATA_TYPE.STRING:
      return { value: Buffer.from(u8.subarray(pos, pos + len)).toString("utf8"), bytesConsumed: headerSize + len };
    case DATA_TYPE.BUFFER:
    case DATA_TYPE.VSBUFFER:
      return { value: u8.slice(pos, pos + len), bytesConsumed: headerSize + len };
    case DATA_TYPE.ARRAY: {
      const arr = [];
      let consumed = headerSize;
      for (let i = 0; i < len; i++) {
        const r = deserializeRpcValue(bytes, offset + consumed);
        arr.push(r.value);
        consumed += r.bytesConsumed;
      }
      return { value: arr, bytesConsumed: consumed };
    }
    case DATA_TYPE.OBJECT:
      return { value: JSON.parse(Buffer.from(u8.subarray(pos, pos + len)).toString("utf8")), bytesConsumed: headerSize + len };
    default:
      throw new Error(`rpc wire: unknown data type tag ${tag}`);
  }
}

export function encodeFrame(type, id, ack, payload) {
  const header = Buffer.alloc(FRAME_HEADER_SIZE);
  header.writeUInt8(type, 0);
  header.writeUInt32BE(id >>> 0, 1);
  header.writeUInt32BE(ack >>> 0, 5);
  header.writeUInt32BE(payload.length, 9);
  return Buffer.concat([header, Buffer.from(payload)]);
}

/** Reassembles SocketProtocol frames from arbitrary byte chunks. */
export class ChunkStream {
  constructor() {
    this.buffer = Buffer.alloc(0);
    this.onFrame = null;
  }
  acceptChunk(chunk) {
    this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)]);
    for (;;) {
      if (this.buffer.length < FRAME_HEADER_SIZE) return;
      const len = this.buffer.readUInt32BE(9);
      const total = FRAME_HEADER_SIZE + len;
      if (this.buffer.length < total) return;
      const frame = this.buffer.subarray(FRAME_HEADER_SIZE, total);
      this.buffer = this.buffer.subarray(total);
      if (this.onFrame && len > 0) this.onFrame(frame);
    }
  }
}

/** Minimal port of upstream ChannelClient: promises + dynamic event listen. */
export class ChannelClient {
  constructor(duplex) {
    this.duplex = duplex;
    this.handlers = new Map();
    this.pendingRejects = new Map();
    this.eventListeners = new Map();
    this.nextId = 0;
    this.initialized = false;
    this.onInitializeCallbacks = [];
    this.onClose = null;
  }
  onData(bytes) {
    const reader = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const header = deserializeRpcValue(bytes, 0);
    const rest = bytes.subarray(header.bytesConsumed);
    const body = rest.length > 0 ? deserializeRpcValue(rest).value : undefined;
    const [type, id] = header.value;
    if (type === RESPONSE.INITIALIZE) {
      this.initialized = true;
      for (const cb of this.onInitializeCallbacks) cb();
      this.onInitializeCallbacks = [];
      return;
    }
    if (type === RESPONSE.EVENT_FIRE) {
      const l = this.eventListeners.get(id);
      if (l) for (const cb of [...l]) cb(body);
      return;
    }
    const handler = this.handlers.get(id);
    if (!handler) return;
    this.handlers.delete(id);
    this.pendingRejects.delete(id);
    if (type === RESPONSE.PROMISE_SUCCESS) handler.resolve(body);
    else if (type === RESPONSE.PROMISE_ERROR) {
      const error = new Error(body?.message ?? "rpc error");
      error.name = body?.name ?? "Error";
      handler.reject(error);
    } else handler.reject(body ?? new Error("rpc error obj"));
  }
  whenInitialized() {
    if (this.initialized) return Promise.resolve();
    return new Promise((resolve) => this.onInitializeCallbacks.push(resolve));
  }
  call(channel, command, args) {
    return this.whenInitialized().then(
      () =>
        new Promise((resolve, reject) => {
          const id = this.nextId++;
          this.handlers.set(id, { resolve, reject });
          this.pendingRejects.set(id, reject);
          this.sendRequest(REQUEST.PROMISE, id, channel, command, args);
        }),
    );
  }
  listen(channel, event, arg) {
    const id = this.nextId++;
    const listeners = new Set();
    this.eventListeners.set(id, listeners);
    const on = (cb) => {
      listeners.add(cb);
      return () => listeners.delete(cb);
    };
    this.whenInitialized().then(() => this.sendRequest(REQUEST.EVENT_LISTEN, id, channel, event, arg));
    return { on };
  }
  sendRequest(type, id, channel, name, arg) {
    const out = [];
    serializeRpcValue(out, [type, id, channel, name]);
    serializeRpcValue(out, arg);
    this.duplex.write(encodeFrame(PROTOCOL_FRAME_TYPE_REGULAR, 0, 0, new Uint8Array(out)));
  }
}

// Connects to the running harness server's channel mux over WebSocket.
// The web UI uses this exact path (packages/server/src/http.ts: GET /ws ->
// setupChannelServer); the server pushes an Initialize frame on open.
export const HOST_CAPABILITY_HEADER = "x-zcode-rpc-host-capability";

// ---- minimal RFC 6455 client framing ----
// Node's built-in WebSocket (undici) silently drops custom headers, and the
// trusted-host route requires the capability header — so we do the upgrade
// with node:http and frame ourselves. Client→server frames must be masked.

export function encodeWsFrame(payload, opcode = 2) {
  const mask = randomBytes(4);
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[1] = 0x80 | len;
  } else if (len < 65_536) {
    header = Buffer.alloc(4);
    header[1] = 0x80 | 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  header[0] = 0x80 | opcode; // FIN + binary
  const masked = Buffer.from(payload);
  for (let i = 0; i < masked.length; i++) masked[i] ^= mask[i & 3];
  return Buffer.concat([header, mask, masked]);
}

export class WsFrameDecoder {
  constructor() {
    this.buffer = Buffer.alloc(0);
    this.onPayload = null;
    this.onClose = null;
  }
  accept(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    for (;;) {
      if (this.buffer.length < 2) return;
      const opcode = this.buffer[0] & 0x0f;
      const masked = (this.buffer[1] & 0x80) !== 0;
      let len = this.buffer[1] & 0x7f;
      let off = 2;
      if (len === 126) {
        if (this.buffer.length < 4) return;
        len = this.buffer.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (this.buffer.length < 10) return;
        len = Number(this.buffer.readBigUInt64BE(2));
        off = 10;
      }
      const maskLen = masked ? 4 : 0;
      if (this.buffer.length < off + maskLen + len) return;
      let payload = this.buffer.subarray(off + maskLen, off + maskLen + len);
      if (masked) {
        const mask = this.buffer.subarray(off, off + 4);
        const un = Buffer.from(payload);
        for (let i = 0; i < un.length; i++) un[i] ^= mask[i & 3];
        payload = un;
      }
      this.buffer = this.buffer.subarray(off + maskLen + len);
      if (opcode === 1 || opcode === 2) this.onPayload?.(payload);
      else if (opcode === 8) {
        this.onClose?.();
        return;
      }
      // ping/pong: not expected server→client on localhost; ignored
    }
  }
}

// The desktop host dispatches as a trusted-host-relay, not a terminal-client:
// a terminal-client scope that touches sessions it doesn't own walks the
// adoption/teardown path that has crashed the server twice. So we take the
// sanctioned route: POST /api/rpc-host-capability (one-time 30s ticket),
// then upgrade at /ws/host with the capability header — the exact connection
// class the desktop scheduler uses (packages/server/src/http.ts).
export async function connectOverWebSocket({
  url,
  token,
  socketFactory,
  fetchImpl = null,
  handshakeTimeoutMs = 15_000,
  log: logger = log,
}) {
  const httpBase = url.replace(/^ws/, "http").replace(/\/ws.*$/, "");
  const capUrl = `${httpBase}/api/rpc-host-capability${token ? `?token=${encodeURIComponent(token)}` : ""}`;
  const doFetch = fetchImpl ?? ((u, init) => fetch(u, init));
  const capResp = await doFetch(capUrl, { method: "POST" });
  if (!capResp.ok) {
    throw new Error(`host capability request failed: HTTP ${capResp.status ?? "?"} ${capUrl}`);
  }
  const capability = (await capResp.json())?.capability;
  if (!capability) {
    throw new Error(`host capability response missing ticket: ${capUrl}`);
  }
  const wsUrl = url.replace(/\/ws$/, "/ws/host");
  const wsHeaders = { [HOST_CAPABILITY_HEADER]: capability };
  const chunkStream = new ChunkStream();
  let socket = null;
  const client = new ChannelClient({
    write: (bytes) => socket?.send(bytes),
  });
  chunkStream.onFrame = (payload) => client.onData(payload);

  if (socketFactory) {
    // test-injected socket: wire up and simulate open
    socket = socketFactory(wsUrl, {
      onOpen: () => {},
      onMessage: (event) => chunkStream.acceptChunk(Buffer.from(event.data)),
      onClose: () => { client.initialized = false; },
      headers: wsHeaders,
    });
    socket.onmessage = (event) => chunkStream.acceptChunk(Buffer.from(event.data));
    socket.onopen?.();
  } else {
    const parsed = new URL(wsUrl);
    let path = parsed.pathname;
    if (token) path += `${parsed.search || "?"}token=${encodeURIComponent(token)}`;
    await new Promise((resolve, reject) => {
      const req = http.request({
        host: parsed.hostname,
        port: parsed.port || 80,
        path,
        headers: {
          Connection: "Upgrade",
          Upgrade: "websocket",
          "Sec-WebSocket-Key": randomBytes(16).toString("base64"),
          "Sec-WebSocket-Version": "13",
          ...wsHeaders,
        },
      });
      const timer = setTimeout(() => {
        req.destroy();
        reject(new Error(`websocket connect timeout: ${wsUrl}`));
      }, handshakeTimeoutMs);
      req.on("response", (res) => {
        clearTimeout(timer);
        reject(new Error(`websocket upgrade refused: HTTP ${res.statusCode} ${path}`));
      });
      req.on("error", (error) => {
        clearTimeout(timer);
        reject(new Error(`websocket connect failed: ${error.message}`));
      });
      req.on("upgrade", (_res, sock) => {
        clearTimeout(timer);
        const decoder = new WsFrameDecoder();
        decoder.onPayload = (bytes) => chunkStream.acceptChunk(bytes);
        sock.on("data", (chunk) => decoder.accept(chunk));
        sock.on("close", () => {
          client.initialized = false;
          logger?.("warn", "server websocket closed");
        });
        sock.on("error", () => {});
        socket = {
          send: (bytes) => sock.write(encodeWsFrame(Buffer.from(bytes))),
          close: () => sock.destroy(),
        };
        resolve();
      });
      req.end();
    });
  }

  // whenInitialized() resolves immediately if Initialize already arrived
  await Promise.race([
    client.whenInitialized(),
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("server initialize timeout")), handshakeTimeoutMs),
    ),
  ]);
  logger?.("info", `server channel connected url=${url}`);
  return { client, close: () => { try { socket?.close?.(); } catch {} } };
}

/** Find the harness server auth token: env first, then the environ of the
 * entry-http process actually LISTENING on the server port (a restarting
 * server changes its token, and stale dying processes must not win). */
export function discoverServerToken({ env = process.env, procRoot = "/proc", serverUrl = null } = {}) {
  const fromEnv = env.ZCODE_SERVER_AUTH_TOKEN?.trim();
  if (fromEnv) return fromEnv;
  try {
    const port = serverUrl ? new URL(serverUrl).port : null;
    const listeningInodes = port ? collectListeningInodes(procRoot, Number(port)) : new Set();
    const pids = readdirSync(procRoot).filter((d) => /^\d+$/.test(d));
    // Two passes: the process OWNING the listening socket on the server port
    // first, then any process carrying the token (every server child inherits
    // the same token, so any match is the right value). No process-name filter:
    // matching on "entry-http" in environ only worked by launcher accident.
    for (const requireSocket of [true, false]) {
      for (const pid of pids) {
        try {
          const pdir = `${procRoot}/${pid}`;
          const environ = readFileSync(`${pdir}/environ`);
          const match = environ.toString("utf8").split("\0").find((kv) => kv.startsWith("ZCODE_SERVER_AUTH_TOKEN="));
          if (!match) continue;
          if (requireSocket && !ownsListeningSocket(pdir, listeningInodes)) continue;
          return match.slice("ZCODE_SERVER_AUTH_TOKEN=".length).trim();
        } catch {}
      }
      if (listeningInodes.size > 0 && requireSocket) continue; // try fallback pass
      break;
    }
  } catch {}
  return null;
}

function collectListeningInodes(procRoot, port) {
  const inodes = new Set();
  const hexPort = port.toString(16).toUpperCase().padStart(4, "0");
  for (const file of ["net/tcp", "net/tcp6"]) {
    try {
      const table = readFileSync(`${procRoot}/${file}`, "utf8");
      for (const line of table.split("\n").slice(1)) {
        const cols = line.trim().split(/\s+/);
        // cols: sl local_address rem_address st ...
        if (cols.length < 10) continue;
        const [addr, p] = cols[1].split(":");
        if (p !== hexPort) continue;
        if (cols[3] !== "0A") continue; // LISTEN only
        inodes.add(cols[9]);
      }
    } catch {}
  }
  return inodes;
}

function ownsListeningSocket(pdir, listeningInodes) {
  if (listeningInodes.size === 0) return false;
  try {
    for (const fd of readdirSync(`${pdir}/fd`)) {
      try {
        const link = readlinkSync(`${pdir}/fd/${fd}`);
        const m = link.match(/^socket:\[(\d+)\]$/);
        if (m && listeningInodes.has(m[1])) return true;
      } catch {}
    }
  } catch {}
  return false;
}

async function pollTurnCompletion(client, sessionTarget, runId, timeoutMs, pollIntervalMs, graceMs, logger) {
  const deadline = Date.now() + timeoutMs;
  let sawActive = false;
  let firstIdleAt = null;
  let attempt = 0;
  while (Date.now() < deadline) {
    attempt += 1;
    try {
      const snapshot = await client.call(AGENT_CHANNEL, "readSession", [sessionTarget]);
      const active = snapshot?.runtime?.activeTurnId;
      if (active === runId) {
        sawActive = true;
        firstIdleAt = null;
      } else if (sawActive) {
        return { outcome: "completed" }; // our turn finished
      } else if (active == null) {
        firstIdleAt ??= Date.now();
        if (Date.now() - firstIdleAt >= graceMs) {
          return { outcome: "completed" }; // turn faster than our first poll
        }
      } else {
        firstIdleAt = null; // a different turn is active; keep waiting for ours
      }
    } catch (error) {
      logger?.("warn", `readSession attempt ${attempt} failed: ${error.message}`);
    }
    await new Promise((r) => setTimeout(r, pollIntervalMs));
  }
  throw new Error(`turn timeout after ${timeoutMs}ms (runId ${runId})`);
}

export function makeProtocolDispatcher({
  url = "ws://127.0.0.1:3030/ws",
  token = null,
  tokenResolver = null,
  timeoutMs = DEFAULT_DISPATCH_TIMEOUT_MS,
  socketFactory = null,
  fetchImpl = null,
  connectTimeoutMs = 15_000,
  pollIntervalMs = 1000,
  graceMs = 20_000,
  fallback = null,
  log: logger = log,
}) {
  let connection = null;
  let connecting = null;

  const getConnection = async () => {
    if (connection) return connection;
    if (!connecting) {
      connecting = connectOverWebSocket({
        url,
        // explicit token wins; discover only when none was provided
        token: token ?? (tokenResolver ? tokenResolver() : null),
        socketFactory,
        fetchImpl,
        handshakeTimeoutMs: connectTimeoutMs,
        log: logger,
      })
        .then((conn) => {
          connection = conn;
          return conn;
        })
        .finally(() => {
          connecting = null;
        });
    }
    return connecting;
  };

  return async function dispatch(automation) {
    if (automation.locationKind && automation.locationKind !== "local") {
      return { ok: false, error: `unsupported location_kind ${automation.locationKind}`, failureKind: "permanent" };
    }
    const sessionId = automation.targetTaskId;
    if (!sessionId) {
      // desktop creates a fresh task for unbound automations; out of scope here —
      // let the CLI dispatcher create a throwaway session instead
      if (fallback) return fallback(automation);
      return { ok: false, error: "automation has no bound session and no fallback dispatcher" };
    }
    let conn;
    try {
      conn = await getConnection();
    } catch (error) {
      connection = null;
      if (fallback) return fallback(automation);
      return { ok: false, error: `server connect failed: ${error.message}` };
    }
    const scheduledAt = automation.nextRunAt ?? automation.retryAt ?? Date.now();
    const runId = `${automation.automationId}:${scheduledAt}`;
    // live-server contract (probed): every session-target service call takes the
    // same flat target incl. the harness-computed workspaceKey from the automation row
    const sessionTarget = {
      sessionId,
      workspacePath: automation.workspacePath,
      workspaceKey: automation.workspaceKey,
      ...(automation.workspaceIdentity ? { workspaceIdentity: automation.workspaceIdentity } : {}),
    };
    let timedOut = false;
    try {
      // safety: dispatching into an already-active session took down a whole
      // server once (resume/steer on a live runtime); busy targets defer instead
      const sessions = await conn.client.call(AGENT_CHANNEL, "listSessions", [
        {
          workspacePath: automation.workspacePath,
          workspaceKey: automation.workspaceKey,
          sessionIds: [sessionId],
        },
      ]);
      const info = Array.isArray(sessions) ? sessions.find((s) => s?.sessionId === sessionId) : undefined;
      if (info?.status === "running") {
        return { ok: false, error: `target session ${sessionId} is busy (running); deferring`, failureKind: "transient" };
      }
      const flow = (async () => {
        await conn.client.call(AGENT_CHANNEL, "resumeSession", [sessionTarget]);
        if (automation.mode) {
          await conn.client.call(AGENT_CHANNEL, "setMode", [{ ...sessionTarget, mode: automation.mode }]);
        }
        await conn.client.call(AGENT_CHANNEL, "sendPrompt", [
          {
            ...sessionTarget,
            content: automation.prompt,
            inputId: runId,
            clientMode: "desktop-continuous",
            automationId: automation.automationId,
          },
        ]);
        // Settle by polling the session snapshot. NEVER use channel listen() for
        // this: EventListen for a dynamic event the scoped service doesn't expose
        // throws inside ChannelServer.onEventListen and kills the whole server
        // process (reproduced against an isolated instance — "Event not found").
        // readSessionEvents is equally unusable on this build: the write path
        // stores event payloads the read schema rejects. So: watch
        // runtime.activeTurnId — ours appearing then clearing = turn completed.
        return await pollTurnCompletion(conn.client, sessionTarget, runId, timeoutMs, pollIntervalMs, graceMs, logger);
      })();
      const event = await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          timedOut = true;
          reject(new Error(`turn timeout after ${timeoutMs}ms`));
        }, timeoutMs);
        flow.then(
          (e) => {
            clearTimeout(timer);
            resolve(e);
          },
          (error) => {
            clearTimeout(timer);
            reject(error);
          },
        );
      });
      if (event.outcome === "completed") {
        return { ok: true, sessionId };
      }
      return { ok: false, error: event.error ?? `turn outcome: ${event.outcome}` };
    } catch (error) {
      if (timedOut) {
        // a wedged connection would poison later dispatches — recycle it
        try {
          conn.close();
        } catch {}
        connection = null;
        return { ok: false, error: error.message };
      }
      // transport-level breakage recovers via fallback; turn failures settle as transient
      if (!clientAlive(conn)) {
        connection = null;
        if (fallback) return fallback(automation);
      }
      return { ok: false, error: error.message };
    }
  };
}

function clientAlive(conn) {
  return conn && conn.client && conn.client.initialized;
}

// ---- real dispatch: run the zcode CLI headless ----

// ---- singleton lock ----

function acquireLock(lockFile) {
  try {
    if (existsSync(lockFile)) {
      const pid = Number(readFileSync(lockFile, "utf8").trim());
      if (Number.isInteger(pid) && pid !== process.pid) {
        try {
          process.kill(pid, 0); // throws if not alive
          log("info", `another driver instance (pid ${pid}) is running; exiting`);
          return false;
        } catch {
          // stale lock — take over
        }
      }
      unlinkSync(lockFile);
    }
    mkdirSync(dirname(lockFile), { recursive: true });
    const fd = openSync(lockFile, "wx");
    writeSync(fd, String(process.pid));
    closeSync(fd);
    return true;
  } catch (error) {
    log("error", `lock acquire failed: ${error.message}`);
    return false;
  }
}

function releaseLock(lockFile) {
  try {
    unlinkSync(lockFile);
  } catch {}
}

// ---- CLI entry ----

function parseArgs(argv) {
  const opts = {
    db: join(homedir(), ".zcode/v2/tasks-index.sqlite"),
    workspace: null,
    timeoutMs: DEFAULT_DISPATCH_TIMEOUT_MS,
    intervalMs: POLL_INTERVAL_MS,
    lockFile: join(homedir(), ".local/state/zcode-cron-driver.lock"),
    once: false,
    dryRun: false,
    serverUrl: process.env.ZCODE_SERVER_URL || "ws://127.0.0.1:3030/ws",
    token: process.env.ZCODE_SERVER_AUTH_TOKEN?.trim() || null,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    if (a === "--once") opts.once = true;
    else if (a === "--dry-run") opts.dryRun = true;
    else if (a === "--server-url") opts.serverUrl = next();
    else if (a === "--token") opts.token = next();
    else if (a === "--db") opts.db = next();
    else if (a === "--workspace") opts.workspace = next();
    else if (a === "--timeout-ms") opts.timeoutMs = Number(next());
    else if (a === "--interval-ms") opts.intervalMs = Number(next());
    else if (a === "--lock-file") opts.lockFile = next();
    else {
      console.error(`unknown argument: ${a}`);
      process.exit(2);
    }
  }
  return opts;
}

export async function main(argv = process.argv.slice(2)) {
  const opts = parseArgs(argv);
  if (!opts.once && !acquireLock(opts.lockFile)) process.exit(0);

  const store = new AutomationStore(opts.db);
  try {
    store.verifySchema();
  } catch (error) {
    log("error", error.message);
    releaseLock(opts.lockFile);
    process.exit(2);
  }

  const dispatch = makeProtocolDispatcher({
    url: opts.serverUrl,
    token: opts.token,
    tokenResolver: () => discoverServerToken({ serverUrl: opts.serverUrl }),
    timeoutMs: opts.timeoutMs,
    log,
  });
  const scheduler = new Scheduler({ store, dispatch, dryRun: opts.dryRun, workspace: opts.workspace });

  let stopping = false;
  const shutdown = (signal) => {
    if (stopping) return;
    stopping = true;
    log("info", `${signal} received; releasing ${scheduler.inFlightIds.size} in-flight claim(s)`);
    clearInterval(timer);
    for (const id of scheduler.inFlightIds) {
      try {
        store.releaseClaim(id);
      } catch {}
    }
    if (!opts.once) releaseLock(opts.lockFile);
    process.exit(0);
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  const tickSafe = async () => {
    try {
      const n = await scheduler.tick();
      if (n > 0) log("info", `tick claimed ${n} automation(s)`);
    } catch (error) {
      log("error", `tick failed: ${error.message}`);
    }
  };

  if (opts.once) {
    await tickSafe();
    store.close();
    return 0;
  }

  log(
    "info",
    `zcode-cron-driver continuous mode: db=${opts.db}${opts.workspace ? ` workspace=${opts.workspace}` : ""} interval=${opts.intervalMs}ms`,
  );
  const timer = setInterval(() => void tickSafe(), opts.intervalMs);
  await tickSafe();
  return new Promise(() => {}); // run until signaled
}

// module executed directly → run CLI; imported → exports only (tests)
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    log("error", `fatal: ${error.message}`);
    process.exit(1);
  });
}
