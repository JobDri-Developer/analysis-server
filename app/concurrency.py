from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass

from app.metrics import task_type_label


@dataclass(frozen=True)
class TaskTypeConcurrencyConfig:
    default_limit: int
    limits_by_task_type: dict[str, int]

    @classmethod
    def from_settings(cls, settings) -> "TaskTypeConcurrencyConfig":
        return cls(
            default_limit=max(int(settings.worker_default_concurrency_limit), 1),
            limits_by_task_type={
                "analysis": max(int(settings.worker_analysis_concurrency_limit), 1),
                "jobposting": max(int(settings.worker_job_posting_concurrency_limit), 1),
            },
        )

    def limit_for(self, task_type: str | None) -> int:
        normalized = task_type_label(task_type)
        return self.limits_by_task_type.get(normalized, self.default_limit)


class TaskTypeConcurrencyLease:
    def __init__(
        self,
        *,
        limiter: "TaskTypeConcurrencyLimiter",
        normalized_task_type: str,
    ) -> None:
        self._limiter = limiter
        self._normalized_task_type = normalized_task_type
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._limiter.release(self._normalized_task_type)


class TaskTypeConcurrencyLimiter:
    def __init__(self, config: TaskTypeConcurrencyConfig) -> None:
        self._config = config
        self._counts: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def try_acquire(self, task_type: str | None) -> TaskTypeConcurrencyLease | None:
        normalized = task_type_label(task_type)
        with self._lock:
            current = self._counts[normalized]
            limit = self._config.limit_for(normalized)
            if current >= limit:
                return None
            self._counts[normalized] = current + 1
        return TaskTypeConcurrencyLease(limiter=self, normalized_task_type=normalized)

    def release(self, normalized_task_type: str) -> None:
        with self._lock:
            current = self._counts.get(normalized_task_type, 0)
            if current <= 1:
                self._counts.pop(normalized_task_type, None)
                return
            self._counts[normalized_task_type] = current - 1

    def limit_for(self, task_type: str | None) -> int:
        return self._config.limit_for(task_type)
