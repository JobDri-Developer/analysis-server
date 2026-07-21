from __future__ import annotations

import json
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from app.api_client import SpringWorkerApiClient
from app.consumer import RabbitMqConsumer
from app.schemas import JobPostingIngestTaskMessage, RetryableWorkerError


class _DummyResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class ObservabilityLoggingTests(unittest.TestCase):
    def test_spring_api_response_log_includes_latency(self) -> None:
        client = SpringWorkerApiClient()
        client._session = MagicMock()
        client._session.post.return_value = _DummyResponse(
            200,
            {
                "isSuccess": True,
                "code": "OK",
                "message": "ok",
                "result": None,
                "error": None,
            },
        )

        with patch("app.api_client.log_info") as log_info_mock, patch(
            "app.api_client.monotonic",
            side_effect=[100.0, 100.125],
        ):
            client._post("/api/internal/test", {"hello": "world"})

        response_log = next(
            call
            for call in log_info_mock.call_args_list
            if len(call.args) > 1 and call.args[1] == "worker.api.response"
        )
        self.assertEqual(response_log.kwargs["latencyMs"], 125)
        self.assertEqual(response_log.kwargs["statusCode"], 200)

    def test_spring_api_failure_log_includes_latency(self) -> None:
        client = SpringWorkerApiClient()
        client._session = MagicMock()
        client._session.get.side_effect = requests.RequestException("boom")

        with patch("app.api_client.log_warning") as log_warning_mock, patch(
            "app.api_client.monotonic",
            side_effect=[200.0, 200.125],
        ):
            with self.assertRaises(RetryableWorkerError):
                client._get("/api/internal/test")

        failure_log = log_warning_mock.call_args
        self.assertEqual(failure_log.kwargs["latencyMs"], 125)
        self.assertEqual(failure_log.kwargs["errorCode"], "SPRING_API_REQUEST_FAILED")

    def test_queue_consume_completed_log_includes_task_processing_latency(self) -> None:
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

        with patch.object(consumer, "_process_job_posting_task") as process_mock, patch.object(
            consumer,
            "_ack_message",
        ) as ack_mock, patch.object(
            consumer,
            "_register_inflight",
            return_value=True,
        ), patch.object(
            consumer,
            "_release_inflight",
        ), patch("app.consumer.log_info") as log_info_mock, patch(
            "app.consumer.monotonic",
            side_effect=[10.0, 10.25],
        ):
            consumer._on_message(channel=MagicMock(), method=method, properties=properties, body=body)

        process_mock.assert_called_once()
        ack_mock.assert_called_once()
        completed_log = next(
            call
            for call in log_info_mock.call_args_list
            if len(call.args) > 1 and call.args[1] == "queue.consume.completed"
        )
        self.assertEqual(completed_log.kwargs["taskProcessingLatencyMs"], 250)

    def test_queue_consume_failed_log_includes_task_processing_latency(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=MagicMock(),
            openai_worker=MagicMock(),
            analysis_openai_worker=MagicMock(),
            recovery_store=MagicMock(),
            terminal_message_store=MagicMock(),
            sleep_fn=lambda _seconds: None,
        )
        message = JobPostingIngestTaskMessage(
            messageId="message-2",
            requestId="request-2",
            taskType="JOB_POSTING_INGEST",
            taskId="task-2",
            userId=1,
            rawText="hello",
            retryCount=0,
            maxRetryCount=3,
            submittedAt=datetime.fromisoformat("2026-07-21T00:00:00+00:00"),
        )
        method = SimpleNamespace(delivery_tag=2, redelivered=False)
        properties = SimpleNamespace(headers={})
        body = json.dumps(message.model_dump(mode="json")).encode("utf-8")

        with patch.object(
            consumer,
            "_process_job_posting_task",
            side_effect=RetryableWorkerError("retry later"),
        ), patch.object(
            consumer,
            "_retry_or_fail",
        ) as retry_mock, patch.object(
            consumer,
            "_register_inflight",
            return_value=True,
        ), patch.object(
            consumer,
            "_release_inflight",
        ), patch("app.consumer.log_warning") as log_warning_mock, patch(
            "app.consumer.monotonic",
            side_effect=[20.0, 20.125],
        ):
            consumer._on_message(channel=MagicMock(), method=method, properties=properties, body=body)

        retry_mock.assert_called_once()
        failed_log = next(
            call
            for call in log_warning_mock.call_args_list
            if len(call.args) > 1 and call.args[1] == "queue.consume.failed"
        )
        self.assertEqual(failed_log.kwargs["taskProcessingLatencyMs"], 125)


if __name__ == "__main__":
    unittest.main()
