"""Loop supervisor: restart a crashed/early-returning loop without taking the
others down, and propagate cancellation (clean shutdown). Backoff sleeps are
patched fast.
"""

from __future__ import annotations

import asyncio

import pytest

from simplex_ingest.supervisor import run_supervised
from simplex_ingest.util import Backoff


@pytest.fixture(autouse=True)
def _fast_backoff(mocker):
    async def _noop(self):
        return 0.0

    mocker.patch.object(Backoff, "sleep", _noop)


class ScriptLoop:
    """A loop whose run() follows a per-invocation script of behaviors."""

    def __init__(self, name, shutdown, behaviors):
        self.name = name
        self._shutdown = shutdown
        self._behaviors = behaviors
        self.calls = 0

    async def run(self):
        b = self._behaviors[min(self.calls, len(self._behaviors) - 1)]
        self.calls += 1
        if b == "raise":
            raise RuntimeError("boom")
        if b == "stop":           # a clean run that also asks for shutdown
            self._shutdown.set()
            return
        if b == "return":         # early return without shutdown -> restart
            return
        if b == "hang":
            await self._shutdown.wait()


async def test_restarts_after_crash_until_shutdown():
    sd = asyncio.Event()
    loop = ScriptLoop("x", sd, ["raise", "raise", "stop"])
    await run_supervised([loop], sd)
    assert loop.calls == 3  # two crashes restarted, third run stops cleanly


async def test_restarts_on_unexpected_early_return():
    sd = asyncio.Event()
    loop = ScriptLoop("x", sd, ["return", "stop"])
    await run_supervised([loop], sd)
    assert loop.calls == 2


async def test_cancellation_propagates_out():
    sd = asyncio.Event()
    loop = ScriptLoop("x", sd, ["hang"])
    task = asyncio.create_task(run_supervised([loop], sd))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_one_crashing_loop_does_not_sink_the_others():
    sd = asyncio.Event()
    crasher = ScriptLoop("crasher", sd, ["raise", "hang"])  # crashes once, then waits
    stopper = ScriptLoop("stopper", sd, ["stop"])           # ends the run
    await run_supervised([crasher, stopper], sd)
    assert stopper.calls == 1
    assert crasher.calls >= 1  # restarted at least once, survived the crash
