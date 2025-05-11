# utils/timer.py
import time
from contextlib import contextmanager

from core.logger import logger

STEP_LOG = 100


class StepTimer:
    def __init__(self):
        self.totals = {}
        self.hits = {}
        self.start = None
        self.last = None
        self.interval = STEP_LOG
        self.count = 0

    @contextmanager
    def time(self, step):
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            self.totals[step] = self.totals.get(step, 0) + elapsed
            self.hits[step] = self.hits.get(step, 0) + 1

            if logger.isEnabledFor(10):  # DEBUG level
                logger.info(f"[TIMER] {step} took {elapsed:.4f}s")

    def tick(self):
        self.count += 1
        if self.count >= self.interval:
            self.log()
            self.reset()

    def log(self):
        avg = {k: v / self.hits.get(k, 1) for k, v in self.totals.items()}
        logger.warning(f"[PERF] Timing measurements: " +
                       ", ".join(f"{k}={v:.4f}s" for k, v in avg.items()))

    def reset(self):
        self.totals.clear()
        self.count = 0
