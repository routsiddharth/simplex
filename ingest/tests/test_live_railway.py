"""Live smoke tests against the deployed Railway instance.

Opt-in only — every test here is marked ``live`` and skipped unless you pass
``--run-live`` (see conftest). They hit the real deployment, so they need
network and, for the CLI-backed ones, an authenticated `railway` linked to the
project.

What's reachable from outside, and why so little is HTTP:
  * The ingest is a DATA process, not an API. The only HTTP server it runs is
    the minimal /health endpoint (same liveness doc on every path) — there is no
    endpoint that serves raw_events/snapshots/edges. So the HTTP test asserts
    end-to-end LIVENESS (all six loops heartbeating).
  * Deeper "is it actually doing work / is data on the volume" checks go through
    the `railway` CLI: scraping recent logs for each loop's activity phrase, and
    stat-ing the DuckDB file on the mounted volume. Those are best-effort: they
    skip (not fail) when the CLI is missing / unauthenticated / a prod read is
    denied, so the live suite still passes from a laptop with only network.
"""

from __future__ import annotations

import shutil
import subprocess
import time

import httpx
import pytest

pytestmark = pytest.mark.live

LIVE_BASE_URL = "https://simplex-production-f4ee.up.railway.app"
RAILWAY_SERVICE = "simplex"
LOOP_NAMES = ["catalog", "websocket", "snapshot", "audit", "discovery", "extraction"]

# Only the 10s-grid marker is guaranteed in a bounded window. Catalog (5 min) and
# discovery/audit (hourly) may fall outside a recent slice, so we don't require
# them — `snapshots emitted` + the snapshot loop tag are the reliable "doing work
# end-to-end" signal.
ACTIVITY_PHRASES = ["snapshots emitted"]


def _railway(*args, timeout=90):
    """Run a `railway` CLI command; return (ok, stdout). ok is False (caller
    skips) when the CLI is absent, unauthenticated, denied, or times out."""
    if shutil.which("railway") is None:
        return False, "railway CLI not installed"
    try:
        proc = subprocess.run(
            ["railway", *args],
            capture_output=True, text=True, timeout=timeout,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return False, f"railway command failed: {exc}"
    if proc.returncode != 0:
        return False, proc.stderr.strip() or "railway command returned non-zero"
    return True, proc.stdout


# -- HTTP: the always-available liveness signal -----------------------------

def _get_health(retries=7, delay=15.0):
    """GET /health, retrying through transient 503s. The discovery loop doesn't
    heartbeat *during* its (≤90s) hourly sweep, and a cold boot has a similar
    window, so health can briefly flicker to 503; retry past it before failing."""
    last = None
    for attempt in range(retries):
        try:
            resp = httpx.get(f"{LIVE_BASE_URL}/health", timeout=15)
            if resp.status_code == 200:
                return resp
            last = f"status {resp.status_code}: {resp.text[:200]}"
        except httpx.HTTPError as exc:
            last = repr(exc)
        if attempt < retries - 1:
            time.sleep(delay)
    raise AssertionError(f"/health never returned 200 after {retries} tries: {last}")


def test_live_health_reports_all_loops_alive():
    body = _get_health().json()
    assert body["healthy"] is True
    assert set(body["loops"]) == set(LOOP_NAMES)
    stale = [name for name, alive in body["loops"].items() if not alive]
    assert not stale, f"loops not heartbeating: {stale}"


def test_live_health_doc_shape():
    resp = httpx.get(f"{LIVE_BASE_URL}/health", timeout=15)
    body = resp.json()
    assert set(body.keys()) == {"healthy", "loops"}
    assert all(isinstance(v, bool) for v in body["loops"].values())


# -- CLI: proof of actual end-to-end work (best-effort) ---------------------

def test_live_logs_show_pipeline_activity():
    ok, out = _railway("logs", "--service", RAILWAY_SERVICE, "--lines", "800")
    if not ok:
        pytest.skip(f"railway logs unavailable: {out}")
    assert out.strip(), "no log lines returned"

    # The data path is alive if the snapshot grid is being emitted and the
    # catalog is refreshing — the two clearest "doing work end-to-end" markers.
    missing = [p for p in ACTIVITY_PHRASES if p not in out]
    assert not missing, f"expected live activity phrases missing from logs: {missing}"
    # At least the persistent ws + snapshot loops should be tagged in recent logs.
    assert 'loop="snapshot"' in out
    assert 'loop="ws"' in out


def test_live_duckdb_is_persisted_on_a_real_volume():
    """Confirm the source-of-truth DuckDB exists, holds data, AND lives on a real
    persistent volume — not the ephemeral container overlay.

    The mount check is the load-bearing one: a file on the overlay disappears on
    every redeploy/restart, taking the durable Stage-3 graph with it. This test
    fails (not skips) when `/data` is not a separate mount, so the misconfig
    can't hide behind a green suite. Fix: `railway volume add --mount-path /data`.
    """
    marker = "---MOUNT---"
    # `railway ssh` space-joins its COMMAND argv and runs it through a remote
    # shell, so pass the whole pipeline as ONE argument (a multi-arg `sh -c ...`
    # gets re-split and mangled).
    ok, out = _railway(
        "ssh", "--service", RAILWAY_SERVICE,
        f"stat -c %s /data/simplex.duckdb; echo {marker}; "
        "grep -q ' /data ' /proc/mounts && echo MOUNTED || echo OVERLAY",
    )
    if not ok:
        pytest.skip(f"railway ssh unavailable / denied: {out}")

    size_part, _, mount_part = out.partition(marker)
    assert int(size_part.strip().splitlines()[-1]) > 0, "DuckDB file is empty"
    assert "MOUNTED" in mount_part, (
        "/data is on the ephemeral container overlay — NO persistent volume is "
        "mounted there, so all data (incl. the durable market_semantics/_edges "
        "graph) is lost on every redeploy. Fix: `railway volume add --mount-path "
        "/data` and redeploy."
    )
