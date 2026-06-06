# CLAUDE.md

Guidance for Claude Code working in this repo. Read this first; follow
[`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) and
[`docs/DEPLOY.md`](./docs/DEPLOY.md) for depth.

## ⚠️ Standing rule: keep the docs in sync

**Any change that affects the architecture or the deployment MUST update the
corresponding doc in the same change — not later, not in a follow-up.**

- Change the **system design** — a loop's behavior or cadence, the data model /
  schema, the data flow, the discovery predicates, an invariant, the
  process/storage model, the venue seam, planned topology → update
  [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md).
- Change the **build / ship / operate** surface — `Dockerfile`, `entrypoint.sh`,
  `railway.json`, env vars, the volume/storage, health/ports, the deploy
  procedure, scaling constraints, rollback/backup → update
  [`docs/DEPLOY.md`](./docs/DEPLOY.md).
- If a change touches both, update both. If you add/remove a constant that
  changes documented behavior, reflect it. Treat a PR/commit that alters these
  surfaces without a matching doc update as **incomplete**.

When in doubt, ask: "would the architecture or deploy doc now be wrong?" If yes,
fix it as part of the change.

## What this is

**Simplex** — a real-time probabilistic coherence engine for prediction markets
(Kalshi). **Only Stage 1 (ingest) is built:** one Python process, five supervised
async loops, embedded DuckDB on a mounted volume. Downstream stages (solver,
graph/viz, R2 export, LLM-in-the-loop trading) are planned — see
[`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) §1 and §11.

The repo is a **monorepo**: the ingest service is self-contained under
[`ingest/`](./ingest/) (its own `Dockerfile`, `pyproject.toml`, `src/`, `tests/`);
`docs/`, `CLAUDE.md`, and `README.md` stay at the repo root. Future services
(`solver` in-process first, then `trader`, `viz`) land as siblings — see
[`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) §10–11. Run dev commands from
`ingest/`.

The five loops: **discovery** (hourly — self-manages the `tracked_series` set via
predicates), **catalog** (5 min — tracked series → active market set),
**websocket** (persistent — orderbook/trade/lifecycle → `raw_events`),
**snapshot** (10 s — replays `raw_events` → `snapshots` grid + book checkpoints),
**audit** (hourly — in-memory book vs REST reconciliation).

## Run / test / inspect

```bash
cd ingest                                  # the service is self-contained under ingest/
python3 -m venv venv && source venv/bin/activate
pip install -e ".[test]"
cp .env.example .env                       # four values; KALSHI_ENV=demo to start
python -m simplex_ingest                   # run the ingest; GET :8080/health
pytest                                     # 116 tests (predicates, DB, loops, orderbook, reconstruct)
python -m simplex_ingest.loops.discovery   # dry-run: print admitted/rejected series
```

Deploy is Railway, single always-on instance — see [`docs/DEPLOY.md`](./docs/DEPLOY.md).

## Constraints & invariants (don't break silently)

1. **`raw_events` is the source of truth** — append-only, never deleted.
   `snapshots` and `book_state` are regenerable from it.
2. **DuckDB is single-writer, single-process.** One process holds the lock; this
   is why ingest is one process and the deploy is **a single instance — never
   scale out**. A second writer (e.g. a future trader) forces a storage split
   (Postgres for OLTP) — see [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) §11.
3. **Discovery owns the tracked set** — no manual allowlist; predicates rule;
   never wipe the working set on a transient empty sweep or REST error.
4. **Behavior is configured in `constants.py`, not env.** Only four secrets/
   deploy values come from `.env` (`config.py`). Don't add env knobs.
5. **No secrets in the image** (`ingest/.dockerignore` excludes `.env`/`*.pem`).
6. **Subscribers must not raise on malformed input** — log and drop.
7. **DuckDB `TIMESTAMP` is naive UTC** — normalize with `util.naive_utc`.

## Conventions

- **Loops** are classes with a `name` and async `run()` (the `runtime.Loop`
  Protocol), supervised by `supervisor.py`; each idles through `util.idle_sleep`
  (periodic heartbeat + early wake on `rt.shutdown`). DB calls go through
  `asyncio.to_thread` (DuckDB is sync + lock-serialized).
- **Logging** is structured JSON to stdout: `log.info("msg", extra={...})`,
  tagged by `loop`. Keep messages greppable (the deploy verification relies on
  exact phrases like `catalog refreshed`, `ws reconciled`, `discovery cycle
  complete`).
- **Tests** live in `tests/` (pytest + hypothesis, `asyncio_mode=auto`). Pure
  logic (predicates) gets exhaustive + property coverage; DB ops get atomicity
  tests; loops get behavior tests over `fake_rest` + a tmp DuckDB. Add tests with
  the code, run `pytest` green before committing.
- **Kalshi specifics** (RSA-PSS auth, bids-only book where `yes_ask = 1 −
  best_no_bid`, WS `seq` semantics, fixed-point dollars) were confirmed against
  live docs — see [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) §6–7.

## Pointers

- Architecture: [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md)
- Deployment & ops: [`docs/DEPLOY.md`](./docs/DEPLOY.md)
- Overview for humans: [`README.md`](./README.md)
