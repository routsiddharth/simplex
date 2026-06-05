# Simplex — Deployment

> **Maintenance:** this file is the source of truth for *how the system is built,
> shipped, and operated*. Any change to the Docker image, `railway.json`,
> `entrypoint.sh`, env surface, volume/storage, health/ports, deploy procedure,
> or scaling constraints **must** update this file in the same change. System
> *design* lives in [`ARCHITECTURE.md`](./ARCHITECTURE.md); the rule is restated
> in the repo-root [`CLAUDE.md`](../CLAUDE.md). This file is the canonical,
> expanded successor to the older `README-DEPLOY.md` quickstart at the repo root.

---

## 1. What gets deployed

A **single always-on container**: one Python process running the five-loop ingest
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

## 2. The image (`Dockerfile`)

Multi-stage, Python **3.13-slim** (matches the dev/tested runtime):

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

## 3. `railway.json`

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
`ON_FAILURE` effectively unbounded. (Switch to `"ALWAYS"` if you want restarts on
clean exits too — not wanted today, since a clean exit is an intentional SIGTERM.)

---

## 4. Required service variables

Set on the Railway service (**Variables** tab or CLI). These four are the entire
runtime env surface; everything else is a constant in `constants.py`.

| Variable | Value | Notes |
|---|---|---|
| `KALSHI_API_KEY_ID` | key id (UUID) | |
| `KALSHI_API_SECRET` | RSA private key (PEM) | multi-line — see below |
| `KALSHI_ENV` | `prod` or `demo` | selects hosts in `config.py` |
| `SIMPLEX_DATA_DIR` | `/data` | **must** match the volume mount path |

Plus, if your platform doesn't inject it: **`PORT=8080`** — the health server
binds `$PORT` when present, else falls back to `HEALTH_PORT` (8080). Railway
injects `$PORT`; set it explicitly only if the healthcheck can't reach the app.

**The multi-line PEM:** paste the whole block (incl. `-----BEGIN…-----` /
`-----END…-----`) into the normal Variables UI (Cmd/Ctrl+Enter for newlines) —
**not** the Raw Editor (sealed values can't be edited there). Formatting need not
be perfect: `config.py::normalize_pem()` recovers the key whether newlines arrive
intact, `\n`-escaped, glued onto one line, or given as a path to a `.pem` file.

Secrets never go in the image — `.dockerignore` excludes `.env`, `.env.*`,
`*.pem`. Locally, copy `.env.example` → `.env`.

---

## 5. Persistent volume (DuckDB lives here)

Create a **10 GB volume mounted at `/data`** and attach it to the service.
**Do not** add a Railway Postgres/managed DB for Stage 1 — DuckDB on the volume
is the store.

```bash
railway volume add --mount-path /data    # then set/grow size to 10 GB in the UI
```

`railway volume add` takes no size flag; set 10 GB in the volume's settings.
Railway auto-injects `RAILWAY_VOLUME_MOUNT_PATH=/data`; we ignore it and read
`SIMPLEX_DATA_DIR` (set to the same `/data`). `entrypoint.sh` chowns `/data` to
`appuser` at startup so writes succeed.

The DB file is `/data/simplex.duckdb`. The volume is durable across
restarts/redeploys but is **not a backup** (see §11).

---

## 6. Disable serverless (runs 24/7)

The persistent WebSocket means the service must **never scale to zero**.
App-sleeping is a **UI toggle**, not a `railway.json` key:

Service → **Settings** → search **"Serverless"** → **OFF** → **Deploy**.
If it won't stick, toggle **on → off → redeploy** to push the real `false`.

---

## 7. Deploying

Two ways. The repo root is the build context; **Root Directory = `/`** (the
Dockerfile and `railway.json` are at the root).

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

After either: build → container start → `/health` returns 200 once all five loops
have a heartbeat (catalog's first REST sweep + discovery's first sweep take a few
seconds). Public/healthcheck URL: service → **Settings → Networking**.

### Project coordinates
- Railway project `simplex` — id `73e893ea-bde1-4547-a9c7-45013d1172c3`
- Service id `11ce35cf-0b31-40ba-a18d-d6580be6d29f`, env `production`
- Last known-good deploy at time of writing: `0e6dbfde` (rollback target)

---

## 8. Health & readiness

`/health` returns `{"healthy": bool, "loops": {...}}`; 200 iff **all five** loops
(`catalog`, `websocket`, `snapshot`, `audit`, `discovery`) heartbeat within
`HEALTH_HEARTBEAT_TIMEOUT_SECONDS` (90 s), else 503. The healthcheck has a 300 s
grace at deploy time.

```bash
curl https://<service-domain>/health
# {"healthy": true, "loops": {"catalog": true, "websocket": true,
#  "snapshot": true, "audit": true, "discovery": true}}
```

---

## 9. Post-deploy verification (do this after a meaningful change)

Watch the logs and confirm, in order — this is the end-to-end cutover signature
(validated locally before first deploy):

1. `discovery cycle complete` — `series_seen=… admitted=… tracked=N` (eager, early).
2. `catalog refreshed` — `series=N active_markets=M` (next catalog tick).
3. `ws reconciled` — `added=M removed=0` (first convergence) / `ws initial subscribe`.
4. `snapshots emitted` — `markets=M` within one `SNAPSHOT_INTERVAL`.
5. `/health` → 200 with all five loops `true`; Railway marks the deploy SUCCESS.

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
for t in ['tracked_series','markets','raw_events','snapshots','book_state','audit_results']:
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
restarts/redeploys but that is not a backup.

1. **Stop → copy (consistent):** stop the service (SIGTERM checkpoints + closes
   the DB cleanly), `railway ssh`, copy `/data/simplex.duckdb` out
   (`railway ssh "base64 /data/simplex.duckdb" | base64 -d > backup.duckdb`).
2. **Live copy is best-effort** — you may catch the WAL mid-write; prefer (1).

Proper off-site backup is the planned **R2 export** (Stage 5), shipping
`snapshots`/`raw_events` continuously. Until then, use (1) periodically.

---

## 12. Local run (parity with prod)

```bash
python3 -m venv venv && source venv/bin/activate
pip install -e ".[test]"
cp .env.example .env        # fill the four values (KALSHI_ENV=demo to start)
python -m simplex_ingest    # five loops; GET :8080/health
pytest                      # 48 tests
python -m simplex_ingest.loops.discovery   # dry-run: print admitted/rejected series
```

Local uses `SIMPLEX_DATA_DIR=./data` by default. The one-shot discovery and a
brief full run against `demo` (or `prod` with a low rate budget) are the
pre-deploy smoke checks.

---

## 13. Future deployment topology (as the system grows)

When the solver and the LLM-trading layer land, this single service becomes a
small **fleet of single-instance services** — `ingest` (autodeploy OK), `solver`,
`trader` (deliberate deploys only) — plus **managed Postgres** for transactional
trade/position state and **object storage** for the historical corpus. The
driver is the single-writer DuckDB constraint plus the very different risk profile
of a money-moving process. Trading timescale (seconds-to-minutes, per the 10 s
grid) means low-latency co-location is **not** required; Railway stays a fit. Full
rationale and the OLTP/OLAP split are in
[`ARCHITECTURE.md`](./ARCHITECTURE.md) §11 — update both files together when that
work begins.
