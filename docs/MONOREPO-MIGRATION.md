# Simplex — Monorepo Migration Plan (Option 1)

> This is the execution plan for relocating the ingest service into a
> self-contained `ingest/` folder. The final section invokes three review skills
> and fixes everything they surface.

---

## Context

Simplex will grow from one process into a small **fleet**: ingest (built),
solver (Stage 3), trader (LLM, money-moving), viz (Stage 4), plus managed
Postgres + R2. The hosting model is **Railway**, whose monorepo mechanism is
per-service **Root Directory** + **Watch Paths** (each service builds from its
own subdirectory and redeploys only when its paths change).

A hard constraint shapes the topology: **DuckDB is single-*process*** — while
ingest holds `simplex.duckdb` open read-write, no other process can open it, not
even read-only. So the solver **cannot** be a separate service reading the live
file; it starts as a 6th in-process loop and only splits out after the storage
inflection (ingest exports snapshots to Postgres/R2). The trader is the first
genuinely separate service (second writer → its own Postgres; money risk → no
autodeploy).

**Option 1** establishes the monorepo pattern at the cheapest possible moment
(only ingest exists; Railway is being configured): relocate the ingest service
into a self-contained `ingest/` folder so future services are siblings. It does
**not** yet extract a shared `libs/simplex_core` — that is deferred to
trader-time (the first second consumer), because the core/ingest boundary is
still partly a guess until the trader's needs are concrete. The recent deepening
work gave most of the clean seams (`BaseSubscriber`, `kalshi/`, `fixedpoint`,
`config`), but the architecture review during this migration found the extraction
is **not yet a clean `git mv`** — a few first-party imports still cross the
boundary (core modules reading `constants`; `runtime.py` mixing `Loop`/
`Heartbeats` with `BookStore`; `fixedpoint` importing `orderbook._q`). The
required pre-extraction cleanup is recorded in
[`ARCHITECTURE.md`](./ARCHITECTURE.md) §11.

Outcome: a self-contained `ingest/` deployable, docs corrected for the real
topology, and a documented path to the fleet — with near-zero churn. *The move
proper* needs no build-file content edits (the code and build files are
move-safe). The only content edits are the dangling-reference cleanup in step 7:
deleting `README-DEPLOY.md` forces stripping its line from `Dockerfile` and
`.dockerignore` — a consequence of the deletion, not of the relocation.

---

## Decision & non-goals

**In scope**
- `git mv` the ingest service into top-level `ingest/` (history preserved).
- Update path references across the docs; correct ARCHITECTURE/DEPLOY topology.
- Set Railway **Root Directory = `ingest`** (+ Watch Paths); no service-count change (still 1 service today).
- Light cleanup of deprecated root files.

**Out of scope (deferred to trader-time, documented as the next step)**
- Extracting `libs/simplex_core`; adding `trader/` / `viz/` folders.
- Provisioning Postgres / R2; writing any solver/trader code.
- Solver remains a future *in-process* loop in `ingest/`.

---

## Target layout

```
BEFORE                                AFTER
repo/                                 repo/
  src/simplex_ingest/                   ingest/                 ← self-contained service
  tests/                                  Dockerfile  entrypoint.sh  railway.json
  pyproject.toml                          .dockerignore  .env.example  pyproject.toml
  Dockerfile entrypoint.sh                src/simplex_ingest/   ← unchanged package
  railway.json .dockerignore              tests/
  .env.example                          docs/  CLAUDE.md  README.md   ← repo-level (stay)
  docs/ CLAUDE.md README.md             .gitignore  .git/  .claude/
                                        (ingest/ pkg name + imports UNCHANGED)
                                        (later: trader/  viz/  libs/simplex_core/)
```

Why the build files need **no content edits**: with Railway **Root Directory =
`ingest`**, the build context *is* `ingest/`. `railway.json` (`dockerfilePath:
"Dockerfile"`) resolves to `ingest/Dockerfile`; the Dockerfile's `COPY
pyproject.toml ./` and `COPY src ./src` resolve against the `ingest/` context;
`db.py` loads `schema.sql` via `Path(__file__).with_name(...)` (relocatable);
`pyproject.toml` uses relative `package-dir = {"" = "src"}`. Verified move-safe.

---

## Migration steps

### 1. Move the service into `ingest/` (use `git mv` to preserve history)
Move these into `ingest/`:
`Dockerfile`, `entrypoint.sh`, `railway.json`, `.dockerignore`, `.env.example`,
`pyproject.toml`, `src/` (→ `ingest/src/simplex_ingest/`), `tests/`.

Keep at repo root: `docs/`, `CLAUDE.md`, `README.md`, `.gitignore`, `.git/`,
`.claude/`. Do **not** move generated dirs (`*.egg-info/`, `.pytest_cache/`,
`.hypothesis/`, `venv/`) — they regenerate.

### 2. Build/deploy files — leave content unchanged
No edits to `Dockerfile`, `railway.json`, `entrypoint.sh`, `pyproject.toml` (see
"Target layout" rationale). `.dockerignore` now sits inside the `ingest/` build
context, which is exactly where it must be.

### 3. Re-create the editable install
The root editable install breaks when `src/` moves. Recreate inside the service:
```
cd ingest && python3 -m venv venv && source venv/bin/activate && pip install -e ".[test]"
```
(Single-venv alternative: from repo root, `pip install -e "ingest/[test]"`.)

### 4. Update doc path references (mechanical)
Same edit pattern everywhere: paths that were repo-root-relative gain an
`ingest/` prefix, and dev commands get a `cd ingest` first. Representative files
and the kinds of edits:
- **docs/DEPLOY.md** (largest, ~15 refs): §2 image paths → `ingest/Dockerfile`
  etc.; §3 `ingest/railway.json`; §7 **Root Directory = `ingest`** (was `/`) +
  build-context note; §12 local run `cd ingest` first.
- **docs/ARCHITECTURE.md**: §10 layout tree → show `ingest/src/simplex_ingest/`
  and the future siblings; `.env.example` → `ingest/.env.example`. (The
  `python -m simplex_ingest…` commands still work, run from `ingest/`.)
- **CLAUDE.md**: the Run/test/inspect block → `cd ingest` before
  `pip install`/`pytest`/`python -m simplex_ingest`; standing-rule file refs.
- **README.md**: repo layout tree + run commands (`cd ingest`).

### 5. Correct the documented topology (substance, not just paths)
- **ARCHITECTURE §11 (Stage 3 — solver):** state explicitly that a *separate*
  solver process cannot open the live DuckDB (single-process lock), so it begins
  **in-process** (6th loop) and only splits out after the OLTP/OLAP storage
  inflection. This corrects the current "split into its own service later"
  framing.
- **ARCHITECTURE §11 (Deployment shape) + DEPLOY §13:** describe the concrete
  Railway fleet — each service = its own **Root Directory + Watch Paths**;
  **ingest autodeploy ON** (read-only, low blast radius, scoped by Watch Paths
  so docs/trader pushes don't rebuild it); **trader autodeploy OFF** (never
  redeploy a process holding open orders); volumes/Postgres/R2 attach
  per-service. Note the build-context wrinkle: once `libs/simplex_core` exists,
  the services that depend on it switch to a repo-root build context + Watch
  Paths (a Dockerfile can only `COPY` from inside its context).
- **Document the deferred core split:** record the intended module boundary plus
  the pre-extraction cleanup the boundary leaks require (the review found the
  split is not yet a clean `git mv`) —
  *core (→ `libs/simplex_core`):* `kalshi/{auth,rest,fixedpoint}`,
  `subscriber.py` (BaseSubscriber), `config.py`, `log.py`, `util.py`,
  `runtime.py` (Loop/Heartbeats only), `supervisor.py`, `health.py`, `events.py`;
  *ingest-specific (stay):* `loops/`, `discovery_predicates.py`, `db.py` +
  `schema.sql`, `constants.py`, `orderbook.py`, `reconstruct.py`, `app.py`,
  `runtime.py`'s `BookStore`/`request_reset`. Leaks to cut first: core reading
  `constants` (`supervisor`/`health`/`rest` → inject), `runtime.py` mixing
  Loop/Heartbeats with `BookStore`, `fixedpoint` importing `orderbook._q`. See
  [`ARCHITECTURE.md`](./ARCHITECTURE.md) §11.

### 6. Railway changes (operator actions — document, don't automate)
In the `simplex` service Settings: **Source → Root Directory = `ingest`**;
**Watch Paths = `ingest/**`** (so docs-only pushes don't redeploy). Unchanged:
the `/data` volume, Serverless OFF, the four service variables, autodeploy ON.
Capture these in DEPLOY §7.

### 7. Cleanup (deprecated root files)
- `README-DEPLOY.md` — superseded by `docs/DEPLOY.md` (it already says so).
  **Delete** (confirm it only points at `docs/DEPLOY.md` before removing). Note:
  deleting it requires stripping its reference from `Dockerfile` and
  `.dockerignore` (both listed it) — the one content edit the cleanup forces.
- `requirements.txt` — legacy; `pyproject.toml` is the source of truth. Delete
  if unreferenced (grep first), else move into `ingest/`.

---

## Verification

Run from `ingest/` after the move:
1. `pip install -e ".[test]"` then `pytest` → **all green, no import breakage**
   (the suite was 116 when this plan was written; it has since grown — 231 today).
2. `python -m simplex_ingest` against `KALSHI_ENV=demo` → `/health` 200; log
   shows the cutover signature (`discovery cycle complete`, `catalog refreshed`,
   `ws reconciled`/`ws initial subscribe`, `snapshots emitted`).
3. `python -m simplex_ingest.loops.discovery` → dry-run prints admitted/rejected.
4. **Docker build** with the new context: `docker build -f ingest/Dockerfile ingest/`
   → the `schema.sql`-packaged assertion passes (proves the move-safe build).
5. **No stale refs:** `grep -rn "src/simplex_ingest" docs/ CLAUDE.md README.md`
   returns only `ingest/`-prefixed paths.
6. **History preserved:** `git log --follow ingest/src/simplex_ingest/app.py`
   shows pre-move commits.
7. **Railway (post-merge):** after setting Root Directory = `ingest`, a deploy
   reaches the §9 post-deploy signature and `/health` 200.

---

## Final reviews — run each, then fix all issues

After the migration is implemented and the suite is green, run these in order
and **fix everything each surfaces** before considering the work done:

1. **`/improve-codebase-architecture`** — invoke it, then fix all issues it
   surfaces (deepening/locality opportunities introduced or revealed by the new
   layout).
2. **`/principal-review`** — invoke it, then fix all issues (architecture,
   correctness, style, consistency of the moved tree + doc edits).
3. **`/security-review`** — invoke it, then fix all issues (ensure the move,
   `.dockerignore` relocation, and any path/build changes introduced no exposure).

Re-run `pytest` (expect all green) after each round of fixes.
