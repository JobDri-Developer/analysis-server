from __future__ import annotations

import json
import os
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY

os.environ.setdefault("APP_WORKER_INTERNAL_API_KEY", "test-internal-key")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

from app.consumer import RabbitMqConsumer
from app.main import metrics
from app.schemas import JobPostingIngestTaskMessage, RetryableWorkerError


def _sample_value(name: str, labels: dict[str, str]) -> float:
    value = REGISTRY.get_sample_value(name, labels=labels)
    return float(value) if value is not None else 0.0


class PrometheusMetricsTests(unittest.TestCase):
    def test_metrics_endpoint_returns_prometheus_payload(self) -> None:
        response = metrics()

        self.assertEqual(response.media_type, CONTENT_TYPE_LATEST)
        body = response.body.decode("utf-8")
        self.assertIn("# HELP worker_queue_wait_duration", body)
        self.assertIn("# HELP worker_processing_duration", body)
        self.assertIn("# HELP llm_request_duration", body)

    def test_inflight_gauge_tracks_task_type(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=MagicMock(),
            openai_worker=MagicMock(),
            analysis_openai_worker=MagicMock(),
            recovery_store=MagicMock(),
            terminal_message_store=MagicMock(),
            sleep_fn=lambda _seconds: None,
        )
        before = _sample_value("worker_inflight_tasks", {"task_type": "analysis"})

        registered = consumer._register_inflight("task-1", "ANALYSIS")
        mid = _sample_value("worker_inflight_tasks", {"task_type": "analysis"})
        consumer._release_inflight("task-1")
        after = _sample_value("worker_inflight_tasks", {"task_type": "analysis"})

        self.assertTrue(registered)
        self.assertEqual(mid, before + 1.0)
        self.assertEqual(after, before)

    def test_retry_processing_metric_is_recorded(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=MagicMock(),
            openai_worker=MagicMock(),
            analysis_openai_worker=MagicMock(),
            recovery_store=MagicMock(),
            terminal_message_store=MagicMock(),
            sleep_fn=lambda _seconds: None,
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
        labels = {"task_type": "jobposting", "outcome": "retry"}
        before = _sample_value("worker_processing_duration_count", labels)

        with patch.object(
            consumer,
            "_process_job_posting_task",
            side_effect=RetryableWorkerError("retry later", failure_reason="INTERNAL_ERROR"),
        ), patch.object(
            consumer,
            "_retry_or_fail",
            return_value="retry",
        ), patch.object(
            consumer,
            "_register_inflight",
            return_value=True,
        ), patch.object(
            consumer,
            "_release_inflight",
        ), patch(
            "app.consumer.monotonic",
            side_effect=[10.0, 10.25],
        ):
            consumer._on_message(channel=MagicMock(), method=method, properties=properties, body=body)

        after = _sample_value("worker_processing_duration_count", labels)
        self.assertEqual(after, before + 1.0)


if __name__ == "__main__":
    unittest.main()
