"""
Adaptation sampler: background task that periodically samples pg_stat_arc
and pg_stat_car to build time-series data of cache adaptation behavior.

Used by the benchmark suite to generate adaptation charts showing how
target_T1_size, list sizes, and hit rates evolve over time.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class AdaptationSample:
    """A single point-in-time sample of cache algorithm stats."""
    timestamp: float          # seconds since sampling started
    metrics: Dict[str, Any]   # stat values at this point


@dataclass
class AdaptationTimeSeries:
    """Complete time-series of adaptation samples for one pool."""
    pool_name: str
    algorithm: str            # "arc", "car", or "lirs"
    samples: List[AdaptationSample] = field(default_factory=list)

    def get_metric_series(self, metric_name: str) -> Tuple[List[float], List[Any]]:
        """Return (timestamps, values) for a specific metric."""
        timestamps = []
        values = []
        for s in self.samples:
            if metric_name in s.metrics:
                timestamps.append(s.timestamp)
                values.append(s.metrics[metric_name])
        return timestamps, values


class AdaptationSampler:
    """Asynchronous background sampler for cache adaptation metrics.

    Usage:
        sampler = AdaptationSampler(db, pool_name, algorithm="arc", interval=0.1)
        await sampler.start()
        # ... run workload ...
        await sampler.stop()
        series = sampler.get_series()
    """

    def __init__(
        self,
        db: Any,  # DatabaseManager
        pool_name: str,
        algorithm: str = "arc",
        interval: float = 0.1,
    ):
        self.db = db
        self.pool_name = pool_name
        self.algorithm = algorithm.lower()
        self.interval = interval
        self._task: Optional[asyncio.Task] = None
        self._series = AdaptationTimeSeries(
            pool_name=pool_name, algorithm=self.algorithm
        )
        self._start_time: float = 0.0
        self._running = False

    async def start(self):
        """Start background sampling."""
        self._series = AdaptationTimeSeries(
            pool_name=self.pool_name, algorithm=self.algorithm
        )
        self._start_time = time.monotonic()
        self._running = True
        self._task = asyncio.create_task(self._sample_loop())
        logger.debug(
            "Started adaptation sampling for %s (%s) at %.1fms interval",
            self.pool_name, self.algorithm, self.interval * 1000,
        )

    async def stop(self):
        """Stop background sampling and collect final sample."""
        self._running = False
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
        logger.debug(
            "Stopped adaptation sampling: %d samples collected",
            len(self._series.samples),
        )

    def get_series(self) -> AdaptationTimeSeries:
        """Return the collected time-series data."""
        return self._series

    async def _sample_loop(self):
        """Periodically sample stats until stopped."""
        while self._running:
            try:
                sample = await self._take_sample()
                if sample:
                    self._series.samples.append(sample)
            except Exception as e:
                logger.debug("Sampling error: %s", e)
            await asyncio.sleep(self.interval)

    async def _take_sample(self) -> Optional[AdaptationSample]:
        """Query the stats view and return a sample."""
        elapsed = time.monotonic() - self._start_time

        if self.algorithm == "arc":
            view = "pg_stat_arc"
        elif self.algorithm == "car":
            view = "pg_stat_car"
        elif self.algorithm == "lirs":
            view = "pg_stat_lirs"
        elif self.algorithm == "lru":
            view = "pg_stat_lru"
        elif self.algorithm == "osic":
            view = "pg_stat_osic"
        else:
            return None

        try:
            row = await self.db.fetchrow(
                f"SELECT * FROM {view} WHERE name = $1",
                self.pool_name,
            )
        except Exception:
            return None

        if not row:
            return None

        metrics = dict(row)
        return AdaptationSample(timestamp=elapsed, metrics=metrics)
