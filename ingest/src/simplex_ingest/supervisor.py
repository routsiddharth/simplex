"""Loop supervisor: restart a crashed loop without taking the others down.

Each loop's ``run()`` is expected to run forever until shutdown. If it raises or
returns early, the supervisor logs and restarts it with jittered backoff (reset
after a sustained healthy run). Cancellation (clean shutdown) propagates out.
"""

from __future__ import annotations

import asyncio
import time

from . import constants as C
from .log import get_logger
from .runtime import Loop
from .util import Backoff

log = get_logger("supervisor")

_HEALTHY_RUN_SECONDS = 60.0  # a run lasting this long resets the backoff


async def _supervise(loop_obj: Loop, shutdown: asyncio.Event) -> None:
    name = loop_obj.name
    backoff = Backoff(
        C.SUPERVISOR_RESTART_MIN_SECONDS,
        C.SUPERVISOR_RESTART_MAX_SECONDS,
        C.SUPERVISOR_RESTART_BACKOFF_FACTOR,
    )
    while not shutdown.is_set():
        started = time.monotonic()
        try:
            await loop_obj.run()
            if shutdown.is_set():
                return
            log.warning("loop returned unexpectedly; restarting", extra={"loop": name})
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("loop crashed; restarting", extra={"loop": name})
        if shutdown.is_set():
            return
        if time.monotonic() - started >= _HEALTHY_RUN_SECONDS:
            backoff.reset()
        delay = await backoff.sleep()
        log.info("loop restart scheduled", extra={"loop": name, "delay_s": round(delay, 2)})


async def run_supervised(loops: list[Loop], shutdown: asyncio.Event) -> None:
    """Run every loop under its own supervisor until shutdown/cancellation."""
    await asyncio.gather(*(_supervise(lp, shutdown) for lp in loops))
