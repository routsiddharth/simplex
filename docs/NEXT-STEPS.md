# Simplex — next steps

Roadmap from the current state: LLM cost migration **implemented in code**, monorepo
migration **done**, Part 3 healthcheck fix **landed** (audit beats per-market mid-sweep),
hermetic suite green (231 passed). **None of it is in production yet** — prod still serves the
previous commit (`17000cd`). Companion docs:
[`END-TO-END.md`](./END-TO-END.md) (what the system does now),
[`LLM-COST-MIGRATION.md`](./LLM-COST-MIGRATION.md),
[`DEPLOY-HEALTHCHECK-AND-VOLUMES.md`](./DEPLOY-HEALTHCHECK-AND-VOLUMES.md).

The steps below are ordered by dependency — each unblocks the next.

## 1. Ship what's now unblocked (the deploy)

Part 3 was the gate: every newer commit (the graph-pruning commit, the cost migration, the
fix itself) was stuck behind the failing healthcheck. With the audit-loop heartbeat fix in,
a redeploy should pass. This is the unblock for everything.

- **Decide: staged vs bundled.** Preferred = **stage them** — deploy the Part 3 fix alone
  first to prove the healthcheck passes in isolation (tiny blast radius), then the cost
  migration on top (large, never-run-in-prod: new provider seam + batch state machine). If a
  bundled deploy misbehaves you have two suspects. If HEAD already bundles both, at minimum
  deploy with the rollback ready and watch the post-deploy signature closely. **This is the
  one open decision — architect's call.**
- **Set `ANTHROPIC_API_KEY` on Railway** before/with the deploy if you want the batch path
  active. Confirm it soft-fails cleanly when unset (degrades to sync, no loop crash) — a
  crash here would re-trigger the healthcheck failure.
- **Watch the post-deploy verification signature** (`DEPLOY.md §9` greppable phrases:
  `catalog refreshed`, `discovery cycle complete`, `extraction cycle complete`, etc.) and
  confirm `/health` → 200 with all six loops.

## 2. Verify the riskiest new piece — batch state-machine restart durability

The batch path persists cross-cycle state in DuckDB (submit on cycle N, reconcile on N+k). A
SIGTERM mid-batch must **not** orphan a submitted Anthropic batch. Before trusting it in prod:
restart the process mid-batch (locally or staging) and confirm it reconciles on recovery
(Anthropic keeps results 29 days). The hermetic suite covers routing/reconcile logic; this
verifies the real restart path.

## 3. Part 4 — resolve the volume state (after a stable deploy)

Gated on a healthy, non-flapping deploy. See
[`DEPLOY-HEALTHCHECK-AND-VOLUMES.md`](./DEPLOY-HEALTHCHECK-AND-VOLUMES.md) Part 4.

- Verify the live mount: `railway ssh -s simplex "du -sh /data; ls -la /data; grep ' /data ' /proc/mounts"`.
- Resolve the `simplex-volume` (0 MB attached) vs `simplex-volume-bflc` (150 MB detached)
  anomaly. **Do not delete either volume** until persistence-across-restart is confirmed.
  Discard any staged "add a second volume" change. Clean up the spurious empty volume only
  after the real one is confirmed.
- Add the `DEPLOY.md` warning: *never create/attach a new volume on this service — it
  silently displaces the one holding the DuckDB.*

## 4. Close the cost loop (the reason the migration exists)

The migration was justified on cost; verify it actually paid off rather than shipping on
faith:
- Use the per-purpose token instrumentation + the dry-run inspector
  (`python -m simplex_ingest.loops.extraction`) to confirm spend dropped.
- Confirm the new tiering holds in prod: **Sonnet on every pair, Opus only on the trust gate**
  (high-confidence pairs), and the **24 h floor** is skipping short-fuse pairs, with
  long-horizon work flowing to the **batch** path and showing the 50% discount.

## 5. Architecture: leave Tier-1/2 deferred

Constants-injection, `runtime.py` split, and `db.py`/`extraction.py` decomposition remain
**deferred to trader-time** per `ARCHITECTURE.md §11` — their forcing function is the second
writer (the trader), which neither the monorepo move nor the in-process solver introduces.
Pursuing them now is speculative restructuring against a topology not yet built.

- **One caveat:** if the new batch/gate tests had to monkeypatch module-level constants to
  exercise routing, that deferred item's pain has arrived early — pull forward only a
  **narrow** constants-injection slice scoped to that path (not a broad sweep), as part of the
  work that introduced the pain. The runtime split and db/extraction decomposition stay fully
  deferred.

## 6. The real next milestone — Stage 4, the solver

Once the fires are out and Stage 3 is cost-sustainable and deployed, build the **solver**:
consume the `trusted`/`soft` `market_edges` as constraints against live prices to surface
probabilistic incoherences. In-process first (single writer, no storage split — so it does
*not* trigger the deferred refactors). This is the first component that produces something
**actionable** rather than infrastructural, and the natural payoff of the graph the system
now builds. See `ARCHITECTURE.md` §1 / §11.

---

### Dependency summary

```
Part 3 fix (done) ─▶ deploy (set ANTHROPIC_API_KEY) ─▶ batch restart-durability check
                                                   └─▶ Part 4 volume cleanup
                                                   └─▶ close cost loop (verify savings)
                                                        └─▶ Stage 4 solver
Tier-1/2 refactors ── deferred to trader-time (out of band)
```
