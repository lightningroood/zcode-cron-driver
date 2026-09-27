// Tests for zcode-cron-driver.mjs — semantics ported from ZCode upstream
// (packages/services/src/session/automationRepo.ts, automationCron.ts, desktop scheduler).
// Run: node zcode-cron-driver.test.mjs (from this file's directory)
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, mkdirSync, writeFileSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  nextCronRun,
  computeScheduleRuleNextRunAt,
  computeAutomationNextRunAt,
  isOneShot,
  computeRetryAt,
  classifyClaim,
  AutomationStore,
  Scheduler,
  SCHEMA_SQL,
  MISFIRE_GRACE_MS,
} from "./zcode-cron-driver.mjs";

// Local-time helper (tests stay timezone-agnostic by building Dates from local parts).
const T = (y, mo, d, h = 0, mi = 0, s = 0) => new Date(y, mo - 1, d, h, mi, s, 0).getTime();
const MIN = 60_000;

// ---- cron next-run math (port of croner usage: strictly later, minute resolution) ----

test("cron: */15 hits the next quarter boundary strictly after from", () => {
  assert.equal(nextCronRun("*/15 * * * *", T(2026, 9, 27, 14, 7, 30)), T(2026, 9, 27, 14, 15));
  assert.equal(nextCronRun("*/15 * * * *", T(2026, 9, 27, 14, 15, 0)), T(2026, 9, 27, 14, 30));
});

test("cron: hour list minute fires within the current hour when still ahead", () => {
  assert.equal(nextCronRun("12 * * * *", T(2026, 9, 27, 14, 7, 30)), T(2026, 9, 27, 14, 12));
  assert.equal(nextCronRun("12 * * * *", T(2026, 9, 27, 14, 12, 30)), T(2026, 9, 27, 15, 12));
});

test("cron: weekday range skips the weekend", () => {
  // 2026-09-27 is a Sunday.
  assert.equal(nextCronRun("0 9 * * 1-5", T(2026, 9, 27, 10, 0)), T(2026, 9, 28, 9, 0));
  // Friday 2026-09-25 10:00 already past that morning's 9am.
  assert.equal(nextCronRun("0 9 * * 1-5", T(2026, 9, 25, 10, 0)), T(2026, 9, 28, 9, 0));
});

test("cron: restricted dom AND dow combine with OR (vixie semantics)", () => {
  // 2026-09-27 is a Sunday. With dom=1 AND dow=Monday both restricted, the
  // earliest OR match is Monday Sep 28 — not Oct 1 (Thursday, dom match).
  assert.equal(nextCronRun("0 0 1 * 1", T(2026, 9, 27)), T(2026, 9, 28));
  // ...and a dom-only match later loses to an earlier dow match.
  assert.equal(nextCronRun("0 0 13 * 5", T(2026, 9, 27)), T(2026, 10, 2)); // first Friday beats Oct 13
});

test("cron: dow 7 aliases sunday", () => {
  assert.equal(nextCronRun("0 0 * * 7", T(2026, 9, 27, 1, 0)), T(2026, 10, 4));
  assert.equal(
    nextCronRun("0 0 * * 7", T(2026, 9, 27, 1, 0)),
    nextCronRun("0 0 * * 0", T(2026, 9, 27, 1, 0)),
  );
});

test("cron: impossible date returns null", () => {
  assert.equal(nextCronRun("0 0 30 2 *", T(2026, 9, 27)), null);
});

test("cron: ? behaves like *", () => {
  assert.equal(nextCronRun("5 ? * * ?", T(2026, 9, 27, 14, 4, 59)), T(2026, 9, 27, 14, 5));
});

test("cron: step with range", () => {
  // hours 8-18/2 → 8,10,12,14,16,18
  assert.equal(nextCronRun("0 8-18/2 * * *", T(2026, 9, 27, 13, 30)), T(2026, 9, 27, 14, 0));
  assert.equal(nextCronRun("0 8-18/2 * * *", T(2026, 9, 27, 14, 0, 1)), T(2026, 9, 27, 16, 0));
});

// ---- scheduleRule math (port of computeScheduleRuleNextRunAt) ----

test("rule: minute interval advances from anchor, never drifts", () => {
  const A = T(2026, 9, 27, 10, 0, 0);
  const rule = { unit: "minute", interval: 15, hour: 10, minute: 0, anchorAt: A };
  assert.equal(computeScheduleRuleNextRunAt(rule, A + 1), A + 15 * MIN);
  assert.equal(computeScheduleRuleNextRunAt(rule, A + 15 * MIN), A + 30 * MIN);
  assert.equal(computeScheduleRuleNextRunAt(rule, A + 16 * MIN), A + 30 * MIN);
  assert.equal(computeScheduleRuleNextRunAt(rule, A - 5 * MIN), A + 15 * MIN);
});

test("rule: hourly aligns to rule.minute on the anchor clock", () => {
  const A = T(2026, 9, 27, 14, 7, 0);
  const rule = { unit: "hourly", interval: 2, hour: 0, minute: 30, anchorAt: A };
  assert.equal(computeScheduleRuleNextRunAt(rule, T(2026, 9, 27, 15, 0)), T(2026, 9, 27, 16, 30));
  assert.equal(computeScheduleRuleNextRunAt(rule, T(2026, 9, 27, 16, 31)), T(2026, 9, 27, 18, 30));
});

test("rule: daily at hh:mm, next strictly future occurrence", () => {
  const rule = { unit: "daily", interval: 1, hour: 9, minute: 0, anchorAt: T(2026, 9, 27, 12) };
  assert.equal(computeScheduleRuleNextRunAt(rule, T(2026, 9, 27, 10, 0)), T(2026, 9, 28, 9, 0));
  assert.equal(computeScheduleRuleNextRunAt(rule, T(2026, 9, 27, 8, 59)), T(2026, 9, 27, 9, 0));
});

test("rule: weekly with ISO weekdays (1=Monday)", () => {
  const rule = {
    unit: "weekly", interval: 1, hour: 9, minute: 0, weekdays: [1],
    anchorAt: T(2026, 9, 21),
  };
  assert.equal(computeScheduleRuleNextRunAt(rule, T(2026, 9, 27, 12, 0)), T(2026, 9, 28, 9, 0));
});

test("rule: monthly by month-day", () => {
  const rule = {
    unit: "monthly", interval: 1, hour: 0, minute: 0, monthDays: [1],
    monthlyMode: "date", anchorAt: T(2026, 9, 1),
  };
  assert.equal(computeScheduleRuleNextRunAt(rule, T(2026, 9, 27)), T(2026, 10, 1, 0, 0));
});

test("computeAutomationNextRunAt prefers scheduleRule over cronExpr", () => {
  const A = T(2026, 9, 27, 10, 0, 0);
  const automation = {
    cronExpr: "*/15 * * * *", // would say A+15m
    scheduleRule: { unit: "minute", interval: 30, hour: 10, minute: 0, anchorAt: A },
  };
  assert.equal(computeAutomationNextRunAt(automation, A + 1), A + 30 * MIN);
  const cronOnly = { cronExpr: "*/15 * * * *", scheduleRule: null };
  assert.equal(computeAutomationNextRunAt(cronOnly, A + 1), A + 15 * MIN);
});

// ---- one-shot / retry / misfire semantics ----

test("isOneShot mirrors upstream definition", () => {
  assert.equal(isOneShot({ recurring: 0, maxRuns: null }), true);
  assert.equal(isOneShot({ recurring: 0, maxRuns: 1 }), true);
  assert.equal(isOneShot({ recurring: 0, maxRuns: 3 }), false);
  assert.equal(isOneShot({ recurring: 1, maxRuns: null }), false);
});

test("computeRetryAt: exponential backoff capped at 15min", () => {
  const now = 1_000_000;
  assert.equal(computeRetryAt(now, 1), now + 30_000);
  assert.equal(computeRetryAt(now, 2), now + 60_000);
  assert.equal(computeRetryAt(now, 3), now + 120_000);
  assert.equal(computeRetryAt(now, 10), now + 900_000);
});

test("classifyClaim: window missed by more than grace = misfire, unless retrying", () => {
  const now = T(2026, 9, 27, 12, 0, 0);
  const base = { recurring: 1, dispatchAttempts: 0 };
  assert.equal(classifyClaim({ ...base, nextRunAt: now - (MISFIRE_GRACE_MS + MIN) }, now), "misfire");
  assert.equal(classifyClaim({ ...base, nextRunAt: now - (MISFIRE_GRACE_MS - MIN) }, now), "dispatch");
  assert.equal(
    classifyClaim(
      { ...base, nextRunAt: now - (MISFIRE_GRACE_MS + MIN), dispatchAttempts: 1 },
      now,
    ),
    "dispatch",
  );
});

// ---- dispatch command construction ----



// ---- store state machine against a scratch SQLite database ----

function scratchStore() {
  const dir = mkdtempSync(join(tmpdir(), "zcron-"));
  const store = new AutomationStore(join(dir, "t.sqlite"));
  AutomationStore.createSchema(store.db);
  return { store, dir };
}

function seed(db, row) {
  db
    .prepare(
      `INSERT INTO automations (automation_id, cron_expr, prompt, workspace_key, workspace_path,
         recurring, max_runs, next_run_at, schedule_rule, created_at, updated_at)
       VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
    )
    .run(
      row.id,
      row.cron ?? "* * * * *",
      row.prompt ?? "p",
      "wk",
      row.workspace ?? "/w",
      row.recurring ?? 1,
      row.maxRuns ?? null,
      row.nextRunAt ?? null,
      row.scheduleRule ?? null,
      Date.now(),
      Date.now(),
    );
}

test("store: claimDue claims only enabled, idle, due rows", () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    seed(store.db, { id: "a-due", nextRunAt: now - 1 });
    seed(store.db, { id: "a-future", nextRunAt: now + 60_000 });
    const claimed = store.claimDue(now);
    assert.deepEqual(claimed.map((r) => r.automation_id), ["a-due"]);
    const row = store.getRow("a-due");
    assert.equal(row.running, 1);
    assert.equal(row.dispatch_status, "claimed");
    assert.equal(row.claimed_at, now);
    // single-flight: nothing to claim again while running
    assert.equal(store.claimDue(now + 1000).length, 0);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("store: stale claims older than CLAIM_STALE_MS are reclaimed", () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    seed(store.db, { id: "a-zombie", nextRunAt: now - 10_000 });
    store.db
      .prepare(`UPDATE automations SET running=1, claimed_at=? WHERE automation_id=?`)
      .run(now - 11 * 60_000, "a-zombie");
    const claimed = store.claimDue(now);
    assert.equal(claimed.length, 1);
    assert.equal(claimed[0].automation_id, "a-zombie");
    assert.equal(store.getRow("a-zombie").claimed_at, now);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("store: past end_at finalizes as completed without dispatch", () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    seed(store.db, { id: "a-ended", nextRunAt: now - 1 });
    store.db
      .prepare(`UPDATE automations SET end_at=? WHERE automation_id=?`)
      .run(now - 1000, "a-ended");
    const claimed = store.claimDue(now);
    assert.deepEqual(claimed, []);
    const row = store.getRow("a-ended");
    assert.equal(row.lifecycle_status, "completed");
    assert.equal(row.enabled, 0);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("store: markDispatched completes a one-shot and reschedules a recurring task", () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    seed(store.db, { id: "a-one", nextRunAt: now - 1, recurring: 0 });
    seed(store.db, { id: "a-rec", nextRunAt: now - 1, recurring: 1 });
    store.claimDue(now);
    store.markDispatched("a-one", { dispatchedAt: now, nextRunAt: null });
    store.markDispatched("a-rec", { dispatchedAt: now, nextRunAt: now + 60_000 });

    const one = store.getRow("a-one");
    assert.equal(one.run_count, 1);
    assert.equal(one.scheduled_run_count, 1);
    assert.equal(one.lifecycle_status, "completed");
    assert.equal(one.enabled, 0);
    assert.equal(one.next_run_at, null);
    assert.equal(one.dispatch_status, "dispatched");
    assert.equal(one.running, 0);

    const rec = store.getRow("a-rec");
    assert.equal(rec.lifecycle_status, "active");
    assert.equal(rec.enabled, 1);
    assert.equal(rec.next_run_at, now + 60_000);
    assert.equal(rec.dispatch_attempts, 0);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("store: markDispatchFailed backs off transient, gives up at max attempts", () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    seed(store.db, { id: "a-f", nextRunAt: now - 1, recurring: 1 });

    store.claimDue(now);
    store.markDispatchFailed("a-f", { failedAt: now, error: "boom", kind: "transient" });
    let row = store.getRow("a-f");
    assert.equal(row.dispatch_attempts, 1);
    assert.equal(row.retry_at, now + 30_000);
    assert.equal(row.running, 0);

    // drive to the cap
    for (let n = 2; n <= 5; n++) {
      store.claimDue(row.retry_at + 1);
      store.markDispatchFailed("a-f", { failedAt: row.retry_at + 1, error: "boom", kind: "transient" });
      row = store.getRow("a-f");
    }
    assert.equal(row.dispatch_status, "idle");
    assert.equal(row.dispatch_attempts, 0);
    assert.equal(row.retry_at, null);
    assert.equal(row.next_run_at, null); // caller passed null nextRunAt
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("store: permanent failure stops the automation", () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    seed(store.db, { id: "a-p", nextRunAt: now - 1 });
    store.claimDue(now);
    store.markDispatchFailed("a-p", { failedAt: now, error: "nope", kind: "permanent" });
    const row = store.getRow("a-p");
    assert.equal(row.lifecycle_status, "failed");
    assert.equal(row.enabled, 0);
    assert.equal(row.running, 0);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("store: skipAndReschedule records a skipped run; finalize ends one-shots", () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    seed(store.db, { id: "a-miss-rec", nextRunAt: now - 30 * MIN, recurring: 1 });
    seed(store.db, { id: "a-miss-one", nextRunAt: now - 30 * MIN, recurring: 0 });

    store.skipAndReschedule({
      automationId: "a-miss-rec",
      runId: "a-miss-rec:" + (now - 30 * MIN),
      workspaceKey: "wk",
      scheduledAt: now - 30 * MIN,
      reason: "computer_asleep_or_app_not_running",
      nextRunAt: now + 15 * MIN,
      finalize: false,
    });
    const run = store.db
      .prepare(`SELECT dispatch_status, error FROM automation_runs WHERE run_id=?`)
      .get("a-miss-rec:" + (now - 30 * MIN));
    assert.equal(run.dispatch_status, "skipped");
    assert.equal(run.error, "computer_asleep_or_app_not_running");
    const rec = store.getRow("a-miss-rec");
    assert.equal(rec.next_run_at, now + 15 * MIN);
    assert.equal(rec.running, 0);
    assert.equal(rec.run_count, 0); // skips never count as runs

    store.skipAndReschedule({
      automationId: "a-miss-one",
      runId: "a-miss-one:" + (now - 30 * MIN),
      workspaceKey: "wk",
      scheduledAt: now - 30 * MIN,
      reason: "computer_asleep_or_app_not_running",
      nextRunAt: null,
      finalize: true,
    });
    const one = store.getRow("a-miss-one");
    assert.equal(one.lifecycle_status, "completed");
    assert.equal(one.enabled, 0);
    assert.equal(one.next_run_at, null);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("store: upsertRunClaimed is idempotent per runId and bumps attempts on retry", () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    store.upsertRunClaimed({
      runId: "r1",
      automationId: "a1",
      workspaceKey: "wk",
      scheduledAt: now,
      trigger: "schedule",
    });
    store.upsertRunClaimed({
      runId: "r1",
      automationId: "a1",
      workspaceKey: "wk",
      scheduledAt: now,
      trigger: "schedule",
    });
    const run = store.db.prepare(`SELECT dispatch_status, attempts FROM automation_runs WHERE run_id=?`).get("r1");
    assert.equal(run.dispatch_status, "claimed");
    assert.equal(run.attempts, 1);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---- scheduler orchestration with a fake dispatcher ----

test("scheduler: missed window is skipped, recurring rescheduled, one-shot finalized", async () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    seed(store.db, { id: "s-rec", nextRunAt: now - 30 * MIN, recurring: 1 });
    seed(store.db, { id: "s-one", nextRunAt: now - 30 * MIN, recurring: 0 });
    const dispatched = [];
    const scheduler = new Scheduler({
      store,
      dispatch: async (a) => {
        dispatched.push(a.automationId);
        return { ok: true };
      },
      now: () => now,
    });
    await scheduler.tick();
    assert.deepEqual(dispatched, []); // both were misfires
    // seed cron is "* * * * *" → rescheduled to the next minute boundary
    const rescheduled = store.getRow("s-rec").next_run_at;
    assert.ok(rescheduled > now && rescheduled <= now + 2 * MIN, `rescheduled=${rescheduled}`);
    assert.equal(store.getRow("s-one").lifecycle_status, "completed");
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("scheduler: due automation is dispatched and settled", async () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    seed(store.db, { id: "s-ok", nextRunAt: now - 30_000, recurring: 1 });
    const dispatched = [];
    const scheduler = new Scheduler({
      store,
      dispatch: async (a) => {
        dispatched.push(a.automationId);
        return { ok: true, sessionId: a.targetTaskId ?? null };
      },
      now: () => now,
    });
    await scheduler.tick();
    assert.deepEqual(dispatched, ["s-ok"]);
    const row = store.getRow("s-ok");
    assert.equal(row.dispatch_status, "dispatched");
    assert.equal(row.run_count, 1);
    assert.equal(row.running, 0);
    assert.ok(row.next_run_at > now); // rescheduled from "* * * * *" cron
    const run = store.db
      .prepare(`SELECT dispatch_status FROM automation_runs WHERE automation_id=?`)
      .get("s-ok");
    assert.equal(run.dispatch_status, "dispatched");
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("scheduler: dispatch failure settles as transient and retries on the next tick", async () => {
  const { store, dir } = scratchStore();
  try {
    let now = Date.now();
    seed(store.db, { id: "s-flaky", nextRunAt: now - 30_000, recurring: 1 });
    let calls = 0;
    const scheduler = new Scheduler({
      store,
      dispatch: async () => {
        calls += 1;
        if (calls === 1) return { ok: false, error: "spawn failed" };
        return { ok: true };
      },
      now: () => now,
    });
    await scheduler.tick();
    let row = store.getRow("s-flaky");
    assert.equal(row.dispatch_status, "failed_to_dispatch");
    assert.equal(row.dispatch_attempts, 1);
    assert.equal(row.retry_at, now + 30_000);

    now = row.retry_at + 1; // advance past backoff
    await scheduler.tick();
    row = store.getRow("s-flaky");
    assert.equal(row.dispatch_status, "dispatched");
    assert.equal(row.run_count, 1);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("store: claimDue with workspace filter only claims that workspace", () => {
  const { store, dir } = scratchStore();
  try {
    const now = Date.now();
    seed(store.db, { id: "a-mine", nextRunAt: now - 1, workspace: "/mine" });
    seed(store.db, { id: "a-other", nextRunAt: now - 1, workspace: "/other" });
    const claimed = store.claimDue(now, "/mine");
    assert.deepEqual(claimed.map((r) => r.automation_id), ["a-mine"]);
    assert.equal(store.getRow("a-other").running, 0);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

// sanity: SCHEMA_SQL actually builds what the store expects
test("SCHEMA_SQL creates automations and automation_runs tables", () => {
  const { store, dir } = scratchStore();
  try {
    const tables = store.db
      .prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'automation%'`)
      .all()
      .map((r) => r.name)
      .sort();
    assert.deepEqual(tables, ["automation_runs", "automations"]);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

// ============================================================================
// protocol layer (desktop-style dispatch via zcode app-server)
// ============================================================================

import {
  encodeWsFrame,
  WsFrameDecoder,
  serializeRpcValue,
  deserializeRpcValue,
  ChunkStream,
  encodeFrame,
  FRAME_HEADER_SIZE,
  ChannelClient,
  connectOverWebSocket,
  makeProtocolDispatcher,
  discoverServerToken,
} from "./zcode-cron-driver.mjs";
import { PassThrough } from "node:stream";

test("rpc wire: serialize/deserialize round-trips tagged values", () => {
  const pairs = [
    ["hello", "hello"],
    [42, 42],
    [[1, "a", { b: 2 }], [1, "a", { b: 2 }]],
    [{ sessionId: "s1", nested: { n: 3 } }, { sessionId: "s1", nested: { n: 3 } }],
    [undefined, undefined],
  ];
  for (const [value, expected] of pairs) {
    const writer = [];
    serializeRpcValue(writer, value);
    const bytes = Uint8Array.from(writer.flat());
    assert.deepEqual(deserializeRpcValue(bytes).value, expected);
    assert.equal(deserializeRpcValue(bytes).bytesConsumed, bytes.length);
  }
});

test("rpc wire: ChunkStream reassembles split frames", () => {
  const stream = new ChunkStream();
  const body = { id: 7 };
  const frame = encodeFrame(1, 0, 0, serializeRpcValueBytes([1, 5, "run1"], body));
  const mid = 10;
  const got = [];
  stream.onFrame = (payload) => got.push(payload);
  stream.acceptChunk(frame.slice(0, mid));
  stream.acceptChunk(frame.slice(mid));
  assert.equal(got.length, 1);
  const parsed = parseFramePayload(got[0]);
  assert.deepEqual(parsed.header, [1, 5, "run1"]);
  assert.deepEqual(parsed.body, { id: 7 });
});

function serializeRpcValueBytes(header, body) {
  const out = [];
  serializeRpcValue(out, header);
  serializeRpcValue(out, body);
  return Uint8Array.from(out.flat());
}
function parseFramePayload(payload) {
  const a = deserializeRpcValue(payload);
  const header = a.value;
  const b = deserializeRpcValue(payload.subarray(a.bytesConsumed));
  return { header, body: b.value };
}
// ChannelClient consumes frame payloads (header already stripped by ChunkStream)
function payloadOf(header, body) {
  const out = [];
  serializeRpcValue(out, header);
  serializeRpcValue(out, body);
  return new Uint8Array(out);
}

test("rpc wire: ChannelClient resolves calls and fires events", async () => {
  const messagesToServer = [];
  const client = new ChannelClient({
    write: (bytes) => messagesToServer.push(Buffer.from(bytes)),
  });
  // server side: reply Initialize, then answer the request
  client.onData(payloadOf([200], undefined));
  const pending = client.call("zcode-agent", "resumeSession", [{ sessionId: "s1" }]);
  await new Promise((r) => setImmediate(r)); // request is written from a microtask
  assert.equal(messagesToServer.length, 1);
  const req = parseFramePayload(messagesToServer[0].subarray(FRAME_HEADER_SIZE));
  assert.equal(req.header[0], 100); // RequestType.Promise
  assert.deepEqual(req.header.slice(2), ["zcode-agent", "resumeSession"]);
  assert.deepEqual(req.body, [{ sessionId: "s1" }]);
  const reqId = req.header[1];
  client.onData(payloadOf([201, reqId], { ok: true }));
  assert.deepEqual(await pending, { ok: true });

  // events: listen → EventFire
  const events = client.listen("zcode-agent", "onDynamicTaskTerminalOutcome", "s2");
  const fired = new Promise((resolve) => events.on((e) => resolve(e)));
  await new Promise((r) => setImmediate(r)); // listen request is written from a microtask
  const listenMsg = parseFramePayload(messagesToServer[1].subarray(FRAME_HEADER_SIZE));
  assert.equal(listenMsg.header[0], 102); // RequestType.EventListen
  assert.equal(listenMsg.body, "s2");
  const listenId = listenMsg.header[1];
  client.onData(payloadOf([204, listenId], { inputId: "r1" }));
  assert.deepEqual(await fired, { inputId: "r1" });
});

test("rpc wire: ChannelClient rejects PromiseError with server message", async () => {
  const client = new ChannelClient({ write: () => {} });
  client.onData(payloadOf([200], undefined));
  const pending = client.call("zcode-agent", "sendPrompt", [{}]);
  await new Promise((r) => setImmediate(r));
  // first call after initialize gets request id 0
client.onData(payloadOf([202, 0], { message: "boom", name: "Error" }));
  await assert.rejects(pending, /boom/);
});

// minimal fake WebSocket server side: speaks the wire protocol over messages
function startFakeWsServer() {
  const state = { listeners: new Map(), sent: [], onmessage: null, onopen: null, onclose: null, binaryType: "arraybuffer" };
  const server = {
    opened: false,
    open() {
      this.opened = true;
      setTimeout(() => {
        state.onopen?.();
        // ChannelServer pushes Initialize on connection open
        state.onmessage?.({ data: encodeFrame(1, 0, 0, bytesOf([200], undefined)) });
      }, 0);
    },
    send(bytes) {
      state.sent.push(Buffer.from(bytes));
      const payload = Buffer.from(bytes).subarray(13);
      const a = deserializeRpcValue(payload);
      const header = a.value;
      const rest = payload.subarray(a.bytesConsumed);
      const body = rest.length ? deserializeRpcValue(rest).value : undefined;
      const [type, id, , command] = header;
      if (type === 100) {
        if (command === "listSessions") {
          const ids = body[0].sessionIds ?? [];
          const list = (state.sessions ?? ids.map((id) => ({ sessionId: id, status: "idle" })));
          setTimeout(() => state.onmessage?.({ data: encodeFrame(1, 0, 0, bytesOf([201, id], list)) }), 1);
        } else if (command === "sendPrompt") {
          state.lastInputId = body[0].inputId;
          state.promptSeen = true;
        } else if (command === "readSession") {
          const activeTurnId = state.activeTurnIdFn ? state.activeTurnIdFn(state) : (state.promptSeen ? state.lastInputId : undefined);
          setTimeout(() => state.onmessage?.({ data: encodeFrame(1, 0, 0, bytesOf([201, id], {
            session: { sessionId: body[0].sessionId },
            runtime: { activeTurnId, pendingRequestIds: [] },
          })) }), 1);
          return;
        }
        setTimeout(() => state.onmessage?.({ data: encodeFrame(1, 0, 0, bytesOf([201, id], { ok: true })) }), 1);
      }
    },
    close() { this.opened = false; },
  };
  function bytesOf(header, body) {
    const o = [];
    serializeRpcValue(o, header);
    serializeRpcValue(o, body);
    return new Uint8Array(o);
  }
  return { server, state };
}

test("ws codec: masked client frames round-trip and split server frames decode", () => {
  const small = Buffer.from("hello");
  const big = Buffer.alloc(70_000, 7); // exercises 16-bit and 64-bit length forms
  for (const payload of [small, big]) {
    const frame = encodeWsFrame(payload);
    const decoder = new WsFrameDecoder();
    const out = [];
    decoder.onPayload = (b) => out.push(Buffer.from(b));
    decoder.accept(frame.subarray(0, 5)); // split delivery
    decoder.accept(frame.subarray(5));
    assert.equal(out.length, 1);
    assert.ok(out[0].equals(payload));
  }
});

test("ws transport: trusted-host route fetches a capability ticket first", async () => {
  const { server, state } = startFakeWsServer();
  const fetchCalls = [];
  const conn = await connectOverWebSocket({
    url: "ws://127.0.0.1:3030/ws",
    token: "tok",
    fetchImpl: async (url, init) => {
      fetchCalls.push({ url, method: init?.method });
      return { ok: true, json: async () => ({ capability: "cap-1", expiresAt: 1 }) };
    },
    socketFactory: (url, opts) => {
      state.lastUrl = url;
      state.onmessage = opts.onMessage; state.onopen = opts.onOpen; server.open(); return server;
    },
  });
  assert.deepEqual(fetchCalls, [
    { url: "http://127.0.0.1:3030/api/rpc-host-capability?token=tok", method: "POST" },
  ]);
  assert.equal(state.lastUrl, "ws://127.0.0.1:3030/ws/host");
  const ok = await conn.client.call("zcode-agent", "ping", []);
  assert.deepEqual(ok, { ok: true });
});

test("ws transport: handshake, framing, call/listen over fake socket", async () => {
  const { server, state } = startFakeWsServer();
  const conn = await connectOverWebSocket({
    url: "ws://127.0.0.1:3030/ws",
    fetchImpl: async () => ({ ok: true, json: async () => ({ capability: "t" }) }),
    socketFactory: (url, opts) => { state.onmessage = opts.onMessage; state.onopen = opts.onOpen; server.open(); return server; },
  });
  const result = await conn.client.call("zcode-agent", "resumeSession", [{ sessionId: "s1" }]);
  assert.deepEqual(result, { ok: true });
  const sub = conn.client.listen("zcode-agent", "onDynamicTaskTerminalOutcome", "s2");
  const fired = new Promise((r) => sub.on((e) => r(e)));
  await new Promise((r) => setTimeout(r, 10));
  state.onmessage?.({ data: encodeFrame(1, 0, 0, bytesOf2([204, 1], { hello: true })) });
  assert.deepEqual(await fired, { hello: true });
  function bytesOf2(header, body) {
    const o = [];
    serializeRpcValue(o, header);
    serializeRpcValue(o, body);
    return new Uint8Array(o);
  }
});

test("protocol dispatcher: resume, setMode, send with desktop identity, settle on turn completion", async () => {
  const { server, state } = startFakeWsServer();
  let polls = 0;
  state.activeTurnIdFn = (s) => {
    polls += 1;
    return polls <= 2 ? s.lastInputId : undefined;
  };
  const dispatcher = makeProtocolDispatcher({
    url: "ws://127.0.0.1:3030/ws",
    timeoutMs: 5000,
    pollIntervalMs: 10,
    graceMs: 4000,
    fetchImpl: async () => ({ ok: true, json: async () => ({ capability: "t" }) }),
    socketFactory: (url, opts) => { state.onmessage = opts.onMessage; state.onopen = opts.onOpen; server.open(); return server; },
  });
  const result = await dispatcher({
    automationId: "auto-1",
    targetTaskId: "sess_abc",
    workspacePath: "/w",
    workspaceKey: "wk",
    mode: "yolo",
    prompt: "do things",
  });
  assert.equal(result.ok, true);
  assert.ok(polls >= 3, "observed active then idle");
  const byCommand = Object.fromEntries(state.sent.map((m) => {
    const payload = m.subarray(13);
    const a = deserializeRpcValue(payload);
    const header = a.value;
    const rest = payload.subarray(a.bytesConsumed);
    const body = rest.length ? deserializeRpcValue(rest).value : undefined;
    return [header[3] ?? "?", body];
  }).filter(([, v]) => v !== undefined));
  assert.deepEqual(byCommand.resumeSession, [{ sessionId: "sess_abc", workspacePath: "/w", workspaceKey: "wk" }]);
  assert.deepEqual(byCommand.setMode, [{ sessionId: "sess_abc", workspacePath: "/w", workspaceKey: "wk", mode: "yolo" }]);
  const sent = byCommand.sendPrompt[0];
  assert.equal(sent.sessionId, "sess_abc");
  assert.equal(sent.workspaceKey, "wk");
  assert.equal(sent.content, "do things");
  assert.equal(sent.clientMode, "desktop-continuous");
  assert.equal(sent.automationId, "auto-1");
  assert.match(sent.inputId, /^auto-1:\d+$/);
});

test("protocol dispatcher: defers when target session is running (never dispatches into an active session)", async () => {
  const { server, state } = startFakeWsServer();
  state.sessions = [{ sessionId: "sess_busy", status: "running" }];
  const dispatcher = makeProtocolDispatcher({
    url: "ws://127.0.0.1:3030/ws",
    timeoutMs: 5000,
    fetchImpl: async () => ({ ok: true, json: async () => ({ capability: "t" }) }),
    socketFactory: (url, opts) => { state.onmessage = opts.onMessage; state.onopen = opts.onOpen; server.open(); return server; },
  });
  const result = await dispatcher({
    automationId: "auto-b", targetTaskId: "sess_busy", workspacePath: "/w", workspaceKey: "wk", mode: "yolo", prompt: "p",
  });
  assert.equal(result.ok, false);
  assert.match(result.error, /busy|running/);
  const commands = state.sent.map((m) => {
    const a = deserializeRpcValue(m.subarray(13));
    return a.value[3];
  });
  assert.equal(commands.includes("resumeSession"), false);
  assert.equal(commands.includes("sendPrompt"), false);
});

test("protocol dispatcher: times out when the turn never finishes", async () => {
  const { server, state } = startFakeWsServer();
  state.activeTurnIdFn = (s) => s.lastInputId;
  const dispatcher = makeProtocolDispatcher({
    url: "ws://127.0.0.1:3030/ws",
    timeoutMs: 5000,
    fetchImpl: async () => ({ ok: true, json: async () => ({ capability: "t" }) }),
    socketFactory: (url, opts) => { state.onmessage = opts.onMessage; state.onopen = opts.onOpen; server.open(); return server; },
  });
  const result = await dispatcher({
    automationId: "auto-2", targetTaskId: "sess_x", workspacePath: "/w", workspaceKey: "wk", mode: null, prompt: "p",
  });
  assert.equal(result.ok, false);
  assert.match(result.error, /timeout/);
});

test("protocol dispatcher: settles via grace when the turn is too fast to observe", async () => {
  const { server, state } = startFakeWsServer();
  state.activeTurnIdFn = () => undefined;
  const dispatcher = makeProtocolDispatcher({
    url: "ws://127.0.0.1:3030/ws",
    timeoutMs: 5000,
    pollIntervalMs: 10,
    graceMs: 50,
    fetchImpl: async () => ({ ok: true, json: async () => ({ capability: "t" }) }),
    socketFactory: (url, opts) => { state.onmessage = opts.onMessage; state.onopen = opts.onOpen; server.open(); return server; },
  });
  const result = await dispatcher({ automationId: "auto-3", targetTaskId: "sess_z", workspacePath: "/w", workspaceKey: "wk", mode: null, prompt: "p" });
  assert.equal(result.ok, true);
});

test("protocol dispatcher: connect failure settles as a transient dispatch failure", async () => {
  const dispatcher = makeProtocolDispatcher({
    url: "ws://127.0.0.1:3030/ws",
    timeoutMs: 500,
    connectTimeoutMs: 200,
    fetchImpl: async () => ({ ok: true, json: async () => ({ capability: "t" }) }),
    socketFactory: () => {
      const s = { send() {}, close() {}, set binaryType(v) {}, get binaryType() { return "arraybuffer"; } };
      return s; // never opens
    },
  });
  const result = await dispatcher({ automationId: "auto-4", targetTaskId: "sess_q", workspacePath: "/w", workspaceKey: "wk", mode: null, prompt: "p" });
  assert.equal(result.ok, false);
  assert.match(result.error, /connect|websocket/i);
});

test("discoverServerToken: env wins, /proc scan as fallback", () => {
  const viaEnv = discoverServerToken({ env: { ZCODE_SERVER_AUTH_TOKEN: "abc" }, procRoot: "/nonexistent" });
  assert.equal(viaEnv, "abc");
});

test("discoverServerToken: picks the entry-http process listening on the server port", () => {
  // synthetic /proc: pid 100 owns the listening socket on :3030 (0xBD6), pid 200 is a stale server
  const dir = mkdtempSync(join(tmpdir(), "zproc-"));
  try {
    mkdirSync(join(dir, "net"), { recursive: true });
    // sl local_address rem_address st inode — LISTEN (0A) on 0.0.0.0:0BD6
    writeFileSync(join(dir, "net", "tcp"),
      "  sl  local_address rem_address   st tx_queue inode  " + "\n" +
      "   0: 00000000:0BD6 00000000:0000 0A 00000000:00000000 00:00000000 0 1000 0 4242 0 0" + "\n");
    for (const [pid, token, ownsSocket] of [["100", "right-token", true], ["200", "stale-token", false]]) {
      const pdir = join(dir, pid);
      mkdirSync(join(pdir, "fd"), { recursive: true });
      writeFileSync(join(pdir, "environ"),
        Buffer.from(`ZCODE_SERVER_AUTH_TOKEN=${token} node entry-http.js `, "utf8"));
      if (ownsSocket) symlinkSync("socket:[4242]", join(pdir, "fd", "3"));
      else symlinkSync("socket:[9999]", join(pdir, "fd", "3"));
    }
    const token = discoverServerToken({ env: {}, procRoot: dir, serverUrl: "ws://127.0.0.1:3030/ws" });
    assert.equal(token, "right-token");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
