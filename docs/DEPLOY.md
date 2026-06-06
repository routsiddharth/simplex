# Simplex — Deployment

> **Maintenance:** this file is the source of truth for *how the system is built,
> shipped, and operated*. Any change to the Docker image, `railway.json`,
> `entrypoint.sh`, env surface, volume/storage, health/ports, deploy procedure,
> or scaling constraints **must** update this file in the same change. System
> *design* lives in [`ARCHITECTURE.md`](./ARCHITECTURE.md); the rule is restated
> in the repo-root [`CLAUDE.md`](../CLAUDE.md). This file is the canonical
> deployment reference (it superseded and replaced an older `README-DEPLOY.md`
> root quickstart).

---

## 1. What gets deployed

A **single always-on container**: one Python process running the six-loop ingest
(see [`ARCHITECTURE.md`](./ARCHITECTURE.md) §2), writing an embedded DuckDB file
to a **mounted persistent volume**. It is env-driven, logs JSON to stdout, and
shuts down cleanly on SIGTERM. Target platform: **Railway**. The same image suits
any single-machine + mounted-volume host (Fly.io, a VM, etc.).

**Hard rule — single instance only.** DuckDB is single-writer and the service
holds one persistent Kalshi WebSocket. **Never run >1 replica** of this service
against the same volume: a second process cannot open the DB read-write, and two
WS connections would double-ingest. Scale **up** (bigger instance) if needed,
never **out**. See [`ARCHITECTURE.md`](./ARCHITECTURE.md) §9.2.

---

## 2. The image (`ingest/Dockerfile`)

Multi-stage, Python **3.13-slim** (matches the dev/tested runtime). The build
context is the **`ingest/` service folder** (Railway **Root Directory = `ingest`**,
§7), so the Dockerfile's `COPY pyproject.toml ./` / `COPY src ./src` resolve
against `ingest/` — no content edits were needed when the service moved into
`ingest/`.

- **builder:** creates `/opt/venv`, `pip install .` from `pyproject.toml` + `src`.
  Asserts `schema.sql` got packaged (`db.py` reads it at runtime) — the build
  fails loudly otherwise.
- **runtime:** slim image, installs `gosu` + `ca-certificates`, creates non-root
  `appuser` (uid 10001), copies the venv. `ENTRYPOINT` is `entrypoint.sh`; `CMD`
  is `python -m simplex_ingest`. `EXPOSE 8080`. `SIMPLEX_DATA_DIR=/data`.

Nothing series-related is baked into the image — the tracked set is discovered at
runtime and persisted in DuckDB on the volume. `certifi`'s CA bundle (in the
venv) is what the `websockets` TLS handshake uses; system `ca-certificates` is
also present.

**`entrypoint.sh`:** Railway mounts volumes owned by root, but the app runs as
`appuser`. The entrypoint `mkdir -p`s + `chown`s `$SIMPLEX_DATA_DIR`, then
`exec gosu appuser "$@"`. Because it `exec`s, the Python process **replaces** the
shell and receives SIGTERM directly for a clean shutdown.

> **Do not** set a custom Start Command in Railway — it bypasses the entrypoint
> and skips the volume-chown + non-root drop. The start command must come from
> the Dockerfile.

---

## 3. `ingest/railway.json`

```json
{
  "build":  { "builder": "DOCKERFILE", "dockerfilePath": "Dockerfile" },
  "deploy": {
    "healthcheckPath": "/health",
    "healthcheckTimeout": 300,
    "restartPolicyType": "ON_FAILURE",
    "restartPolicyMaxRetries": 1000000
  }
}
```

Build from the Dockerfile; healthcheck `/health` with a 300 s window; restart
`ON_FAILURE` effectively unbounded. With **Root Directory = `ingest`** (§7),
`dockerfilePath: "Dockerfile"` resolves to `ingest/Dockerfile` — the path is
relative to the service root, so it needs no change. (Switch to `"ALWAYS"` if you want restarts on
clean exits too — not wanted today, since a clean exit is an intentional SIGTERM.)

---

## 4. Required service variables

Set on the Railway service (**Variables** tab or CLI). These five are the entire
runtime env surface (four required + one optional); everything else is a constant
in `constants.py`.

| Variable | Value | Notes |
|---|---|---|
| `KALSHI_API_KEY_ID` | key id (UUID) | |
| `KALSHI_API_SECRET` | RSA private key (PEM) | multi-line — see below |
| `KALSHI_ENV` | `prod` or `demo` | selects hosts in `config.py` |
| `SIMPLEX_DATA_DIR` | `/data` | **must** match the volume mount path |
| `OPENROUTER_API_KEY` | OpenRouter key | **optional** — enables the Stage-3 LLM extraction layer; absent, that loop idles and plain ingest runs unchanged. The only secret beyond Kalshi; model ids/tuning are constants. |

Plus, if your platform doesn't inject it: **`PORT=8080`** — the health server
binds `$PORT` when present, else falls back to `HEALTH_PORT` (8080). Railway
injects `$PORT`; set it explicitly only if the healthcheck can't reach the app.

**The multi-line PEM:** paste the whole block (incl. `-----BEGIN…-----` /
`-----END…-----`) into the normal Variables UI (Cmd/Ctrl+Enter for newlines) —
**not** the Raw Editor (sealed values can't be edited there). Formatting need not
be perfect: `config.py::normalize_pem()` recovers the key whether newlines arrive
intact, `\n`-escaped, glued onto one line, or given as a path to a `.pem` file.

Secrets never go in the image — `ingest/.dockerignore` excludes `.env`, `.env.*`,
`*.pem`. Locally, copy `ingest/.env.example` → `ingest/.env` (§12).

---

## 5. Persistent volume (DuckDB lives here)

Create a **10 GB volume mounted at `/data`** and attach it to the service.
**Do not** add a Railway Postgres/managed DB for Stage 1 — DuckDB on the volume
is the store.

> ⚠️ **The volume is mandatory and easy to forget.** Without it, `SIMPLEX_DATA_DIR=/data`
> resolves to the **ephemeral container overlay**: the DB still writes and the app
> looks healthy, but **every redeploy/restart silently wipes everything** —
> including the durable, costly-to-rebuild `market_semantics`/`market_edges` graph.
> Verify with `railway ssh -s <svc> "grep ' /data ' /proc/mounts"` (a line means a
> real volume is mounted; no line means overlay). The opt-in live test
> `test_live_railway.py::test_live_duckdb_is_persisted_on_a_real_volume`
> (`pytest --run-live`) asserts exactly this and fails loudly when the volume is
> missing.

```bash
railway volume add --mount-path /data    # then set/grow size to 10 GB in the UI
```

`railway volume add` takes no size flag; set 10 GB in the volume's settings.
Railway auto-injects `RAILWAY_VOLUME_MOUNT_PATH=/data`; we ignore it and read
`SIMPLEX_DATA_DIR` (set to the same `/data`). `entrypoint.sh` chowns `/data` to
`appuser` at startup so writes succeed.

The DB file is `/data/simplex.duckdb`. The volume is durable across
restarts/redeploys but is **not a backup** (see §11).

**Sizing.** The time-series tables are pruned to a rolling window (the discovery
loop enforces `DATA_RETENTION_CYCLES`, ≈3 h of `raw_events`/`snapshots`/
`audit_results`/`book_state` — see [`ARCHITECTURE.md`](./ARCHITECTURE.md) §9.1), so
the bulk of the DB no longer grows without bound; 10 GB is ample headroom. The
only monotonically-growing store is the durable LLM graph
(`market_semantics`/`market_edges`), which is small per row and is exactly why the
volume must be a *real* persistent mount.

---

## 6. Disable serverless (runs 24/7)

The persistent WebSocket means the service must **never scale to zero**.
App-sleeping is a **UI toggle**, not a `railway.json` key:

Service → **Settings** → search **"Serverless"** → **OFF** → **Deploy**.
If it won't stick, toggle **on → off → redeploy** to push the real `false`.

---

## 7. Deploying

**Service source settings (set once).** The ingest service builds from its own
subdirectory, so in Service → **Settings → Source**:

- **Root Directory = `ingest`** — the build context is the `ingest/` folder, so
  `Dockerfile`, `railway.json`, and `.dockerignore` resolve inside it. (This was
  `/` before the service was relocated into `ingest/`.)
- **Watch Paths = `ingest/**`** — only redeploy when ingest's own files change.
  Docs-only or future `trader/`/`viz/` pushes then don't rebuild ingest. This is
  the per-service monorepo mechanism (each service = Root Directory + Watch
  Paths); see §13.

Two ways to deploy:

### Option A — GitHub autodeploy on push (current setup)
Service → **Settings → Source → Connect Repo** → `routsiddharth/simplex`, branch
**`main`**. Ensure **Automatic Deploys** is ON. Railway then builds from the
Dockerfile and **redeploys on every push to `main`**.

> Connecting the repo deploys whatever is on `main` *now*. Autodeploy is fine for
> this low-blast-radius read-only service. Reconsider it for any future
> money-moving service (don't redeploy a process holding open orders) — see
> [`ARCHITECTURE.md`](./ARCHITECTURE.md) §11.

### Option B — CLI (manual, decoupled from git)
```bash
railway link                              # once
railway up --service simplex --detach     # build + deploy current dir
```

CLI deploys are **not** git-triggered — independent of pushes. Use this when you
want explicit control over when a deploy happens.

After either: build → container start → `/health` returns 200 once all six loops
have a heartbeat (catalog's first REST sweep + discovery's first sweep take a few
seconds; extraction heartbeats immediately, idle or not). Public/healthcheck URL:
service → **Settings → Networking**.

### Project coordinates
- Railway project `simplex` — id `73e893ea-bde1-4547-a9c7-45013d1172c3`
- Service id `11ce35cf-0b31-40ba-a18d-d6580be6d29f`, env `production`
- Last known-good deploy at time of writing: `0e6dbfde` (rollback target)

---

## 8. Health & readiness

`/health` returns `{"healthy": bool, "loops": {...}}`; 200 iff **all six** loops
(`catalog`, `websocket`, `snapshot`, `audit`, `discovery`, `extraction`)
heartbeat within `HEALTH_HEARTBEAT_TIMEOUT_SECONDS` (90 s), else 503. The
healthcheck has a 300 s grace at deploy time. `extraction` reports healthy even
without `OPENROUTER_API_KEY` (it heartbeats while idle).

```bash
curl https://<service-domain>/health
# {"healthy": true, "loops": {"catalog": true, "websocket": true,
#  "snapshot": true, "audit": true, "discovery": true, "extraction": true}}
```

---

## 9. Post-deploy verification (do this after a meaningful change)

Watch the logs and confirm, in order — this is the end-to-end cutover signature
(validated locally before first deploy):

1. `discovery cycle complete` — `series_seen=… admitted=… tracked=N` (eager, early).
2. `catalog refreshed` — `series=N active_markets=M` (next catalog tick).
3. `ws reconciled` — `added=M removed=0` (first convergence) / `ws initial subscribe`.
4. `snapshots emitted` — `markets=M` within one `SNAPSHOT_INTERVAL`.
5. **If `OPENROUTER_API_KEY` is set:** `semantics extracted` (per market), then
   `edge classified` (per pair), then `extraction cycle complete`
   — `semantics=… edges=…` (within one `EXTRACTION_INTERVAL`, after the catalog
   first populates markets). Without the key: `extraction layer disabled (loop
   idles)` once, and the loop still heartbeats.
6. `/health` → 200 with all six loops `true`; Railway marks the deploy SUCCESS.

**Rotation (one-time sanity, ≥ 1 h):** after a discovery interval + one catalog
refresh, expect a fresh `discovery cycle complete` and — if Kalshi's catalog
moved — a `ws reconciled` with non-zero `added`/`removed`. Both zero just means
the catalog didn't change; that's correct, not a bug.

**Restart preserves state:** restart the service; logs should show
`loaded checkpoints` (non-zero) and `replay cursor set`, i.e. it resumed from
`book_state` rather than replaying from scratch.

---

## 10. Operating: logs, inspecting the DB, rollback

```bash
railway logs                # stream JSON logs (one object/line; tagged by `loop`)
railway logs --build        # build logs
railway logs -n 200         # last 200 lines
railway status --json       # latestDeployment.status: SUCCESS | FAILED | ...
```

**Inspect the DuckDB file** (read-only — the app holds the read-write lock; a
second read-write open *and* a read-only open both fail while it runs, so this is
best-effort and may need a brief stop):
```bash
railway ssh
python - <<'PY'
import duckdb
c = duckdb.connect('/data/simplex.duckdb', read_only=True)
for t in ['tracked_series','markets','raw_events','snapshots','book_state',
          'audit_results','market_semantics','market_edges']:
    print(t, c.execute(f'SELECT count(*) FROM {t}').fetchone()[0])
PY
```

**Rollback:** `railway rollback` to the last known-good deployment (e.g.
`0e6dbfde`) from the dashboard/CLI. Roll back if, after the 300 s grace:
healthcheck fails for 5 min; any loop reports dead >5 min steady-state; DuckDB
write/schema errors; or a loop crash-loops (supervisor restarts > 5/min).

---

## 11. Backups

Railway has **no volume snapshot CLI** today. The volume survives
restarts/redeploys but that is not a backup. Backups matter more now that the DB
holds the **non-regenerable** Stage-3 tables (`market_semantics`/`market_edges`):
unlike `snapshots`/`book_state` they can't be rebuilt from `raw_events` (they cost
model spend + carry human review decisions — see
[`ARCHITECTURE.md`](./ARCHITECTURE.md) §9.6), so a lost volume loses real work.
The file copy below captures them along with everything else.

1. **Stop → copy (consistent):** stop the service (SIGTERM checkpoints + closes
   the DB cleanly), `railway ssh`, copy `/data/simplex.duckdb` out
   (`railway ssh "base64 /data/simplex.duckdb" | base64 -d > backup.duckdb`).
2. **Live copy is best-effort** — you may catch the WAL mid-write; prefer (1).

Proper off-site backup is the planned **R2 export** (Stage 5), shipping
`snapshots`/`raw_events` continuously. Until then, use (1) periodically.

---

## 12. Local run (parity with prod)

```bash
cd ingest                   # the service is self-contained under ingest/
python3 -m venv venv && source venv/bin/activate
pip install -e ".[test]"
cp .env.example .env        # fill the 4 Kalshi values (KALSHI_ENV=demo); OPENROUTER_API_KEY optional
python -m simplex_ingest    # six loops; GET :8080/health
pytest                      # 146 tests
python -m simplex_ingest.loops.discovery     # dry-run: print admitted/rejected series
python -m simplex_ingest.loops.extraction    # dry-run: extraction work plan (ingest stopped)
```

Local uses `SIMPLEX_DATA_DIR=./data` by default (relative to `ingest/`). The one-shot discovery and a
brief full run against `demo` (or `prod` with a low rate budget) are the
pre-deploy smoke checks.

---

## 13. Future deployment topology (as the system grows)

The `ingest/` relocation (this doc's current state) establishes the monorepo
pattern: **one repo, per-service Root Directory + Watch Paths**, each service a
sibling folder building from its own subdirectory. Today there is exactly **one**
service (`ingest`); the layout makes the rest additive.

When the solver and the LLM-trading layer land, this becomes a small **fleet of
single-instance services**, each its own Railway service pointed at a sibling
folder:

| Service | Root Directory | Autodeploy | Storage | Notes |
|---|---|---|---|---|
| `ingest` | `ingest` | **ON** | `/data` volume (DuckDB) | read-only, low blast radius; Watch Paths `ingest/**` keep docs/trader pushes from rebuilding it |
| `trader` | `trader` | **OFF** | own **Postgres** | money-moving; never redeploy a process holding open orders — deliberate deploys only |
| `viz` | `viz` | ON | — / R2 | presentation only |

Plus **managed Postgres** for transactional trade/position state and **object
storage (R2)** for the historical corpus, **attached per-service** (each service
mounts only what it needs). The driver for splitting at all is the single-writer
DuckDB constraint plus the very different risk profile of a money-moving process;
the **solver starts as the 7th in-process loop inside `ingest/`** (the Stage-3
extraction loop is the 6th, already shipped; both stay in-process because a
separate process cannot open the live DuckDB) and only splits out after the
OLTP/OLAP storage inflection. Trading timescale (seconds-to-minutes, per the 10 s
grid) means low-latency co-location is **not** required; Railway stays a fit.

**Build-context wrinkle (future).** Once a shared `libs/simplex_core` is
extracted (deferred to trader-time), any service that depends on it can no longer
build from its own subdirectory — a Dockerfile only `COPY`s from inside its build
context. Those services switch to a **repo-root build context** (Root Directory
`/`) with **Watch Paths scoped** to their folder + `libs/**`, while
`ingest`-without-the-split can stay rooted at `ingest/`. Plan this when the split
happens.

Full rationale and the OLTP/OLAP split are in
[`ARCHITECTURE.md`](./ARCHITECTURE.md) §11 — update both files together when that
work begins.
