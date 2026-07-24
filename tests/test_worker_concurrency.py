from __future__ import annotations

import json
import os
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

os.environ.setdefault("APP_WORKER_INTERNAL_API_KEY", "test-internal-key")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

from app.concurrency import TaskTypeConcurrencyConfig, TaskTypeConcurrencyLimiter
from app.consumer import RabbitMqConsumer
from app.schemas import JobPostingIngestTaskMessage


class WorkerConcurrencyTests(unittest.TestCase):
    def test_task_type_concurrency_config_resolves_per_task_limits(self) -> None:
        config = TaskTypeConcurrencyConfig(
            default_limit=3,
            limits_by_task_type={"analysis": 2, "jobposting": 4},
        )

        self.assertEqual(config.limit_for("ANALYSIS"), 2)
        self.assertEqual(config.limit_for("JOB_POSTING_INGEST"), 4)
        self.assertEqual(config.limit_for("UNKNOWN_TASK"), 3)

    def test_consumer_requeues_when_task_type_limit_is_reached(self) -> None:
        limiter = TaskTypeConcurrencyLimiter(
            TaskTypeConcurrencyConfig(default_limit=1, limits_by_task_type={"jobposting": 1, "analysis": 1})
        )
        lease = limiter.try_acquire("JOB_POSTING_INGEST")
        self.assertIsNotNone(lease)

        consumer = RabbitMqConsumer(
            api_client=MagicMock(),
            openai_worker=MagicMock(),
            analysis_openai_worker=MagicMock(),
            recovery_store=MagicMock(),
            terminal_message_store=MagicMock(),
            sleep_fn=lambda _seconds: None,
            concurrency_limiter=limiter,
        )
        message = JobPostingIngestTaskMessage(
            messageId="message-1",
            requestId="request-1",
            taskType="JOB_POSTING_INGEST",
            taskId="task-1",
            userId=1,
            rawText="hello",
            retryCount=0,
            maxRetryCount=3,
            submittedAt=datetime.fromisoformat("2026-07-21T00:00:00+00:00"),
        )
        method = SimpleNamespace(delivery_tag=1, redelivered=False)
        properties = SimpleNamespace(headers={})
        body = json.dumps(message.model_dump(mode="json")).encode("utf-8")

        with patch.object(
            consumer,
            "_register_inflight",
            return_value=True,
        ), patch.object(
            consumer,
            "_release_inflight",
        ) as release_mock, patch.object(
            consumer,
            "_nack_message",
        ) as nack_mock, patch.object(
            consumer,
            "_process_job_posting_task",
        ) as process_mock:
            consumer._on_message(channel=MagicMock(), method=method, properties=properties, body=body)

        process_mock.assert_not_called()
        nack_mock.assert_called_once()
        release_mock.assert_called_once_with("task-1")
        lease.release()


if __name__ == "__main__":
    unittest.main()
