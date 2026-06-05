# Deploying Simplex stage 1 ingest to Railway

> **Canonical deploy doc is now [`docs/DEPLOY.md`](./docs/DEPLOY.md)** — it is the
> maintained superset (env, volume, deploy methods, verification, rollback,
> scaling constraints, future topology). This file remains as the original
> Railway-setup quickstart; prefer `docs/DEPLOY.md` when they differ.

This is the deployment layer only. The app is already env-driven, logs JSON to
stdout, and shuts down cleanly on SIGTERM. Files: `Dockerfile`, `entrypoint.sh`,
`railway.json`, `.dockerignore`.

## 1. Required service variables

Set these in the Railway service's **Variables** tab (or via CLI). The four from
the spec are all you need — the health server binds Railway's injected `$PORT`
automatically (falling back to 8080 if unset):

| Variable | Value | Notes |
|---|---|---|
| `KALSHI_API_KEY_ID` | your key id (UUID) | |
| `KALSHI_API_SECRET` | the RSA private key (PEM) | multi-line — see below |
| `KALSHI_ENV` | `prod` or `demo` | |
| `SIMPLEX_DATA_DIR` | `/data` | must match the volume mount path |

### Setting the multi-line PEM (`KALSHI_API_SECRET`)

Railway's variable editor supports multi-line values (press **Cmd/Ctrl+Enter**
for newlines, or just paste the whole block). Use the **normal Variables UI**,
not the Raw Editor — sealed/secret values can't be edited via the Raw Editor.

Paste the entire key including the `-----BEGIN RSA PRIVATE KEY-----` and
`-----END...-----` lines.

You don't have to get the newlines perfect: `config.py`'s `normalize_pem()`
recovers the key whether newlines arrive intact, `\n`-escaped, or flattened onto
one line. So a paste that loses formatting will still work.

CLI alternative for the simple ones:
```bash
railway variables --set "KALSHI_API_KEY_ID=..." --set "KALSHI_ENV=prod" \
                  --set "SIMPLEX_DATA_DIR=/data"
```
(Do the PEM through the UI — multi-line values are painful to pass through a shell.)

---

## 2. Persistent volume (DuckDB lives here)

We use DuckDB on a mounted volume — **do not** add a Railway Postgres/managed DB.

Create a 10 GB volume mounted at `/data` and attach it to this service:

```bash
railway volume add --mount-path /data
# then in the Railway UI: open the volume → set size to 10 GB
```
(or UI: service → **New Volume** → mount path `/data`). `railway volume add`
doesn't take a size flag; set/grow the 10 GB in the volume's settings. Railway
auto-injects `RAILWAY_VOLUME_MOUNT_PATH=/data` — we don't use it; we read
`SIMPLEX_DATA_DIR` instead, which you set to the same `/data`.

The container runs as non-root (`appuser`); `entrypoint.sh` chowns `/data` to it
at startup (Railway mounts volumes as root), so writes succeed.

---

## 3. Disable serverless (this service runs 24/7)

The persistent WebSocket means the service must never scale to zero. App-sleeping
is a **UI toggle**, not a `railway.json` key:

Service → **Settings** → search **"Serverless"** → toggle **OFF** → **Deploy**.

Known gotcha: if it won't stick, toggle it **on, then off, then redeploy** to
push the real `false` to the backend.

---

## 4. Deploy

`railway.json` already pins: build from `Dockerfile`, healthcheck `/health`
(300s timeout), restart `ON_FAILURE` with `restartPolicyMaxRetries: 1000000`
(the schema has no max → effectively unbounded; swap to `"ALWAYS"` if you want
restarts even on clean exits).

> The start command intentionally comes from the Dockerfile
> (`entrypoint.sh → gosu appuser → python -m simplex_ingest`). **Don't** set a
> custom Start Command in Railway — it would bypass the entrypoint and skip the
> volume-chown + non-root drop.

**Option A — connect the repo (recommended):** Railway dashboard → New Project →
Deploy from GitHub repo → pick this repo. Railway detects `railway.json` +
`Dockerfile` and redeploys on every push.

**Option B — CLI:**
```bash
railway link        # link to the project/service once
railway up          # build + deploy current dir (use --detach to not tail logs)
```

After deploy: build succeeds → container starts → `/health` returns 200 once all
four loops have a heartbeat (catalog's first REST sweep can take a bit). Your
public/healthcheck URL is in the service's **Settings → Networking**.

---

## 5. Logs

```bash
railway logs            # stream runtime logs (structured JSON, one object/line)
railway logs --build    # build logs
railway logs -n 200     # last 200 lines
```
Or the **Deployments** view in the dashboard. Each line is tagged with `loop`
and relevant IDs.

---

## 6. SSH in / inspect the DuckDB file

```bash
railway ssh             # interactive shell in the running container
# (or copy the exact command: dashboard → right-click service → Copy SSH Command)
```

Inside the container, inspect the DB **read-only** (the app holds the
read-write lock; a second read-write open will fail):
```bash
python - <<'PY'
import duckdb
c = duckdb.connect('/data/simplex.duckdb', read_only=True)
for t in ['markets','raw_events','snapshots','book_state','audit_results']:
    print(t, c.execute(f'SELECT count(*) FROM {t}').fetchone()[0])
PY
```

---

## 7. Back up the volume

Railway has **no volume snapshot/backup CLI** today. The volume itself is durable
across restarts/redeploys, but that's not a backup. Options, best-consistency first:

1. **Stop → copy (consistent):** stop the service (SIGTERM checkpoints books and
   closes the DB cleanly), then `railway ssh` in and copy `/data/simplex.duckdb`
   somewhere, or stream it out:
   ```bash
   railway ssh "base64 /data/simplex.duckdb" | base64 -d > simplex-backup.duckdb
   ```
2. **Live export (best-effort):** while running, you can't open the DB read-write
   from a second process, and copying the live file may catch the WAL mid-write.
   Prefer option 1 for a clean snapshot.

Proper off-site backup is the **R2 export job** (a later stage), which will ship
`snapshots`/`raw_events` out continuously. Until then, use option 1 periodically.

---

## 8. Verify volume persistence across a restart (acceptance #6)

1. Let it run long enough to checkpoint (≥ `CHECKPOINT_INTERVAL_SECONDS`, 60s) —
   look for `"msg":"checkpointed books"` in logs.
2. `railway ssh` and note `SELECT count(*) FROM book_state`.
3. Restart the deployment (dashboard → **Restart**, or redeploy).
4. After it comes back, `railway ssh` again and confirm `book_state` still has the
   prior rows — and logs show `"msg":"loaded checkpoints"` with a non-zero count
   and a recent `replay cursor set`, i.e. it resumed instead of replaying from scratch.
