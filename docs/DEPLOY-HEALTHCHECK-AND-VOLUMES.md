# Railway incident handoff — failed-deploy healthcheck (Part 3) + volume state (Part 4)

Status: **Part 3 (healthcheck) FIXED 2026-06-06** — audit loop now beats per
market mid-sweep (`loops/audit.py`), with a regression test and a `DEPLOY.md §8`
note; the rolled-back commit should now pass the healthcheck on redeploy.
**Part 4 (volume state) still open** — an operator action on Railway, no code
change. Working handoff doc. Investigated 2026-06-06.
Deploy target: Railway project `simplex`, environment `production`, service `simplex`,
URL `https://simplex-production-f4ee.up.railway.app`, US East, 1 replica.

The system is **up and serving** — but on the *previous* commit (see Part 3). An earlier
red herring (OpenRouter `402` credit exhaustion) has already been resolved by topping up
credits; it is noted below only because it appears in the failed-deploy logs.

---

## Part 3 — latest deploy fails its Railway healthcheck and rolls back

### Symptom

The latest commit never went live. Railway failed it at the healthcheck step and kept the
prior deployment active, so **production is running one commit behind**.

| Deployment ID | Commit | When (local +03:00) | Result |
|---|---|---|---|
| `1b797902-f44f-4976-940f-d91265805a93` | `123ab79` "feat: prune the LLM graph 1h after a market resolves" (**latest**) | 2026-06-06 15:28 | **FAILED** — `Network › Healthcheck` (04:34) → "Healthcheck failure" |
| `2883c65a-5d91-4d81-a5e2-aca6d257a9f2` | `17000cd` "feat: rolling-window data retention…" | 2026-06-06 14:33 | **SUCCESS** — currently ACTIVE / serving |

`railway.json`: `healthcheckPath: "/health"`, `healthcheckTimeout: 300`.
`/health` returns 200 iff **all six** loops (`catalog`, `websocket`, `snapshot`, `audit`,
`discovery`, `extraction`) heartbeat within the per-loop timeout (~90 s — documented as
`HEALTH_HEARTBEAT_TIMEOUT_SECONDS` in `DEPLOY.md §8`; confirm the constant's location),
within the 300 s deploy grace.

### Evidence (from the failed deploy's logs)

Pulled with:

```bash
railway deployment list
railway logs 1b797902-f44f-4976-940f-d91265805a93 -d --lines 400
```

The app **started and ran** (loops active), but:

1. A massive synchronous **startup orderbook REST sweep** — thousands of
   `GET .../markets/KXNBADRAFTPICK-26-N-XXX/orderbook?depth=100` lines. The NBA-draft
   series (`KXNBADRAFTPICK`, many pick numbers × ~50 player outcomes each) had **exploded
   the tracked market set**.
2. `ws connection error … keepalive ping timeout (1011)` → `ws reconnecting delay_s=29.6`.
   Websocket flapping — the signature of event-loop / rate-budget contention during the sweep.
3. `402 Payment Required` from OpenRouter (credit exhaustion — **since resolved**; soft-fails
   and skips, does not by itself fail the healthcheck).
4. Ends with `shutdown signal received signal="SIGTERM"` → `shutting down` → `Stopping
   Container`. **Railway SIGTERM'd it** because `/health` never returned 200 within the grace —
   not a self-crash.

### Root-cause hypothesis (to confirm)

The orderbook sweep is the **audit loop** — `loops/audit.py`, constants
`AUDIT_ORDERBOOK_DEPTH = 100`, `AUDIT_REST_CALLS_PER_SECOND = 4.0`,
`AUDIT_TICK_SECONDS = 3600` (hourly, in-memory book vs REST reconciliation).

At 4 REST calls/sec, sweeping a market set that the NBA-draft series blew up to thousands of
markets takes **many minutes**. The most likely failure mode: **the audit loop does not
heartbeat *during* its sweep**, so on the initial startup tick its heartbeat goes stale for
longer than the ~90 s timeout → `/health` stays 503 for the entire 300 s grace → Railway
fails the deploy. The earlier deploy passed because the market set was smaller and the sweep
finished (or beat) inside the window.

Contrast: the **extraction** loop beats per item
(`self.rt.heartbeats.beat(self.name)` inside its batch loop). Check whether **audit** does
the same; if it only beats at end-of-cycle, that's the bug.

### Fix directions (for whoever picks this up — pick the smallest that holds)

1. **Heartbeat mid-sweep (cheapest, preferred).** Have the audit loop call
   `self.rt.heartbeats.beat(self.name)` periodically as it walks markets (mirroring
   extraction), so a long sweep keeps `/health` green. Verify the same for any other loop
   whose first tick can exceed ~90 s on a large market set.
2. **Throttle / defer / chunk the initial audit sweep** so startup isn't dominated by a
   single multi-minute reconciliation (e.g. stagger the first tick, or cap markets-per-tick
   and round-robin).
3. **Investigate the ws keepalive timeout** — confirm it's a *symptom* of the sweep
   contention and not an independent ws-loop bug that also withholds a heartbeat.
4. **Last resort:** raise `railway.json healthcheckTimeout` (currently 300). Band-aid only —
   it hides a startup that scales badly with market-set size; the market set will keep
   growing.

> Note: this interacts with the LLM cost migration — that work tightens which markets get
> tracked/extracted, but the audit sweep covers the *catalog* market set, so the healthcheck
> fix is independent and should land first (any redeploy of the latest commit will keep
> failing until the heartbeat-during-sweep issue is fixed).

### What's been done

- Identified the rollback and the two deployment IDs (`railway deployment list`).
- Pulled and analysed the failed deploy's logs; isolated the SIGTERM-after-healthcheck
  pattern, the audit orderbook sweep, and the ws keepalive timeout.
- Traced the sweep to `loops/audit.py` via the `AUDIT_ORDERBOOK_DEPTH=100` constant.
- **Fix landed (fix direction 1):** `run_pass()` now calls
  `self.rt.heartbeats.beat(self.name)` once per market in the sweep loop, so a
  long catalog sweep keeps `/health` green. Added a regression test
  (`test_audit_beats_per_market_during_sweep`) and a `DEPLOY.md §8` note.
  Hermetic suite green (`pytest` → 231 passed, 4 skipped).

---

## Part 4 — volume state (likely benign, do NOT touch blind)

### Observed state

`railway volume list`:

| Volume | ID | Attached to | Mount | Used |
|---|---|---|---|---|
| `simplex-volume` | `13c3f65c-9163-4776-ba5f-a341bb846043` | `simplex` | `/data` | **0 MB** |
| `simplex-volume-bflc` | — | **N/A (detached)** | `/tmp` | **150 MB** |

The app writes `/data/simplex.duckdb`: Dockerfile bakes `ENV SIMPLEX_DATA_DIR=/data`,
`entrypoint.sh` defaults `DATA_DIR="${SIMPLEX_DATA_DIR:-/data}"`, `config.py` →
`db_path = data_dir / "simplex.duckdb"`. `railway variables` confirms
`RAILWAY_VOLUME_MOUNT_PATH=/data`, `RAILWAY_VOLUME_ID=13c3f65c`.

### The anomaly

The **attached** `/data` volume is **0 MB** despite active DB writes, while **150 MB** sits
on the **detached** `/tmp` volume. Candidate explanations (unresolved):

- **(a) Metric lag** — Railway's volume-usage figure is stale/cached. Most likely benign.
- **(b) Config drift** — the running container mounted a different volume than the
  currently-configured one (a new empty `simplex-volume` was attached at some point;
  `simplex-volume-bflc` holds the real DB from before).
- **(c) Old data** — the 150 MB is a prior DuckDB from when a volume was at `/tmp`.

Could **not** verify the live mount: `railway ssh` is gated as a production read and the
container state was unstable during investigation, so `/data` contents were never inspected.

### Important cautions

- There was a staged **"Apply 2 changes"** in the dashboard attempting to attach a **second**
  volume → Railway error: *"A service can only have one volume."* **Discard those staged
  changes; do not Deploy them.** Adding a second volume is never the fix.
- **Do NOT delete either volume.** The 150 MB may be the only copy of the LLM graph
  (`market_semantics` / `market_edges`), which `DEPLOY.md` calls "non-regenerable and costly
  to rebuild." `raw_events`/`snapshots` on it are pruned/regenerable; the graph is not.
- Detaching a volume on Railway is **reversible**; deleting is not.

### Resolution path (do this only after Part 3 is fixed and the deploy is stable)

1. Verify the running mount:
   ```bash
   railway ssh -s simplex "du -sh /data; ls -la /data; grep ' /data ' /proc/mounts"
   ```
2. Branch on the result:
   - **`/data` ≈ 150 MB with `simplex.duckdb`** → healthy, running on the real data; the
     `simplex-volume` 0 MB was just config/metric noise. Clean up the spurious empty volume
     **after** confirming persistence survives a restart.
   - **`/data` empty / no `simplex.duckdb`** → real data is stranded on `simplex-volume-bflc`.
     `railway volume detach` the empty `simplex-volume`, `railway volume attach`
     `simplex-volume-bflc` at `/data`, redeploy, re-verify. **Caveat:** `bflc` currently shows
     mount `/tmp` — confirm the DuckDB file layout *inside* it before relying on it (the file
     may be at a `/tmp`-relative path, not the volume root).
3. Only after live persistence is confirmed working (and a restart preserves it — see
   `DEPLOY.md` "restart preserves state"), delete the unused volume to stop paying for it.

### Suggested `DEPLOY.md` addition

This wasn't a code regression — it was a volume displaced at the Railway level. Add an
explicit warning: **"never create/attach a new volume on this service — it silently
displaces the one holding the DuckDB (one-volume-per-service limit)."**

### What's been done

- Enumerated both volumes (`railway volume list`, twice) and the injected
  `RAILWAY_VOLUME_*` vars.
- Confirmed the expected DB path from `Dockerfile` / `entrypoint.sh` / `config.py`.
- Attempted live `/data` inspection via `railway ssh` — **blocked** (production read).
- **No volume mutations made.**

---

## Diagnostic commands used (reproducible)

```bash
railway status
railway variables
railway volume list
railway deployment list
railway logs --service simplex --lines 1500          # active deploy, recent
railway logs <FAILED_DEPLOY_ID> -d --lines 400        # failed deploy startup logs
railway ssh -s simplex "<cmd>"                        # production read — needs approval
```

Greppable log phrases (structured JSON-ish, `[LEVEL] msg key="val"`):
`extraction cycle complete` (with `semantics=`/`edges=`), `semantics extracted`,
`edge classified`, `discovery cycle complete`, `catalog refreshed`, `ws reconciled`,
`shutdown signal received`. A healthy-idle extraction cycle logs
`extraction cycle complete … semantics=0 edges=0` — that is normal no-work, not a fault.
