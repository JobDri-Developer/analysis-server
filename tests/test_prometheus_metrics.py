from __future__ import annotations

import json
import os
import types
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY

os.environ.setdefault("APP_WORKER_INTERNAL_API_KEY", "test-internal-key")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

from app.consumer import RabbitMqConsumer
from app.main import metrics
from app.metrics import DURATION_BUCKETS, increment_llm_request_error, observe_internal_api
from app.openai_client import APIStatusError, AnalysisOpenAiWorker, JobPostingOpenAiWorker
from app.schemas import (
    AnalysisTaskMessage,
    AnalysisWorkerContextResponse,
    JobPostingClassificationResultResponse,
    JobPostingClassificationCandidateResponse,
    JobPostingExtractResponse,
    JobPostingIngestTaskMessage,
    NonRetryableWorkerError,
    RetryableWorkerError,
)


def _sample_value(name: str, labels: dict[str, str]) -> float:
    value = REGISTRY.get_sample_value(name, labels=labels)
    return float(value) if value is not None else 0.0


class PrometheusMetricsTests(unittest.TestCase):
    def test_metrics_endpoint_returns_prometheus_payload(self) -> None:
        response = metrics()

        self.assertEqual(response.media_type, CONTENT_TYPE_LATEST)
        body = response.body.decode("utf-8")
        self.assertIn("# HELP worker_task_queue_wait_duration_seconds", body)
        self.assertIn("# HELP worker_task_processing_duration_seconds", body)
        self.assertIn("# HELP llm_request_duration_seconds", body)
        self.assertIn("# HELP worker_internal_api_duration_seconds", body)

    def test_inflight_gauge_tracks_task_type(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=MagicMock(),
            openai_worker=MagicMock(),
            analysis_openai_worker=MagicMock(),
            recovery_store=MagicMock(),
            terminal_message_store=MagicMock(),
            sleep_fn=lambda _seconds: None,
        )
        before = _sample_value("worker_task_inflight", {"task_type": "analysis"})

        registered = consumer._register_inflight("task-1", "ANALYSIS")
        mid = _sample_value("worker_task_inflight", {"task_type": "analysis"})
        consumer._release_inflight("task-1")
        after = _sample_value("worker_task_inflight", {"task_type": "analysis"})

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
        before = _sample_value("worker_task_processing_duration_seconds_count", labels)

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

        after = _sample_value("worker_task_processing_duration_seconds_count", labels)
        self.assertEqual(after, before + 1.0)

    def test_internal_api_and_llm_error_metrics_are_recorded(self) -> None:
        api_labels = {
            "task_type": "analysis",
            "endpoint": "analysis_context",
            "method": "POST",
            "outcome": "failed",
        }
        llm_labels = {
            "task_type": "analysis",
            "operation": "analysis-final",
            "error_type": "rate_limit",
        }
        api_before = _sample_value("worker_internal_api_duration_seconds_count", api_labels)
        llm_before = _sample_value("worker_llm_request_errors_total", llm_labels)

        observe_internal_api("ANALYSIS", "analysis_context", "POST", "failed", 0.25)
        increment_llm_request_error("ANALYSIS", "analysis-final", "RATE_LIMIT")

        api_after = _sample_value("worker_internal_api_duration_seconds_count", api_labels)
        llm_after = _sample_value("worker_llm_request_errors_total", llm_labels)

        self.assertEqual(api_after, api_before + 1.0)
        self.assertEqual(llm_after, llm_before + 1.0)

    def test_duration_buckets_extend_beyond_observed_queue_wait_ceiling(self) -> None:
        self.assertGreater(max(DURATION_BUCKETS), 300.0)

    def test_job_posting_classify_fallback_records_fallback_llm_outcome(self) -> None:
        worker = JobPostingOpenAiWorker.__new__(JobPostingOpenAiWorker)
        worker._task_type = "JOB_POSTING_INGEST"
        worker._model = "test-model"
        worker._create_response = lambda **_kwargs: types.SimpleNamespace(output_text='{"reason":"bad"}')  # type: ignore[method-assign]

        candidates = [
            JobPostingClassificationCandidateResponse(
                detailClassificationId=1,
                detailClassificationName="Backend",
                middleClassificationName="Server",
                bigClassificationName="Engineering",
                score=0.9,
            )
        ]
        fallback_labels = {
            "task_type": "jobposting",
            "operation": "job-posting-classify",
            "outcome": "fallback",
        }
        error_labels = {
            "task_type": "jobposting",
            "operation": "job-posting-classify",
            "error_type": "validation_error",
        }
        fallback_before = _sample_value("llm_request_duration_seconds_count", fallback_labels)
        error_before = _sample_value("worker_llm_request_errors_total", error_labels)

        result = worker.classify(
            JobPostingExtractResponse(
                companyName="JobDri",
                jobTitle="Backend Engineer",
                task="Build APIs",
                requirements="Python",
                preferredQualifications="Testing",
                rawText="raw",
                confidence=0.9,
            ),
            candidates,
        )

        fallback_after = _sample_value("llm_request_duration_seconds_count", fallback_labels)
        error_after = _sample_value("worker_llm_request_errors_total", error_labels)

        self.assertEqual(result.detailClassificationId, 1)
        self.assertEqual(fallback_after, fallback_before + 1.0)
        self.assertEqual(error_after, error_before + 1.0)

    def test_job_posting_generate_fallback_records_fallback_llm_outcome(self) -> None:
        worker = JobPostingOpenAiWorker.__new__(JobPostingOpenAiWorker)
        worker._task_type = "JOB_POSTING_INGEST"
        worker._model = "test-model"
        worker._create_response = lambda **_kwargs: types.SimpleNamespace(output_text="[]")  # type: ignore[method-assign]

        fallback_labels = {
            "task_type": "jobposting",
            "operation": "job-posting-generate",
            "outcome": "fallback",
        }
        error_labels = {
            "task_type": "jobposting",
            "operation": "job-posting-generate",
            "error_type": "validation_error",
        }
        fallback_before = _sample_value("llm_request_duration_seconds_count", fallback_labels)
        error_before = _sample_value("worker_llm_request_errors_total", error_labels)

        result = worker.generate(
            JobPostingExtractResponse(
                companyName="JobDri",
                jobTitle="Backend Engineer",
                task="Build APIs",
                requirements="Python",
                preferredQualifications="Testing",
                rawText="raw",
                confidence=0.9,
            ),
            JobPostingClassificationResultResponse(
                detailClassificationId=1,
                detailClassificationName="Backend",
                middleClassificationName="Server",
                bigClassificationName="Engineering",
                confidence=0.9,
            ),
        )

        fallback_after = _sample_value("llm_request_duration_seconds_count", fallback_labels)
        error_after = _sample_value("worker_llm_request_errors_total", error_labels)

        self.assertEqual(result.companyName, "JobDri")
        self.assertEqual(fallback_after, fallback_before + 1.0)
        self.assertEqual(error_after, error_before + 1.0)

    def test_analysis_validation_error_is_non_retryable(self) -> None:
        worker = AnalysisOpenAiWorker.__new__(AnalysisOpenAiWorker)
        worker._task_type = "ANALYSIS"
        worker._model = "test-model"
        worker._client = types.SimpleNamespace(
            responses=types.SimpleNamespace(create=lambda **_kwargs: types.SimpleNamespace(output_text='{"jobFit": 1}'))
        )

        failed_labels = {
            "task_type": "analysis",
            "operation": "analysis-final",
            "outcome": "failed",
        }
        error_labels = {
            "task_type": "analysis",
            "operation": "analysis-final",
            "error_type": "validation_error",
        }
        failed_before = _sample_value("llm_request_duration_seconds_count", failed_labels)
        error_before = _sample_value("worker_llm_request_errors_total", error_labels)

        with self.assertRaises(NonRetryableWorkerError) as cm:
            worker.analyze(
                AnalysisWorkerContextResponse(
                    userId=1,
                    mockApplyId=1,
                    companyName="JobDri",
                    jobTitle="Backend Engineer",
                    task="Build APIs",
                    requirements="Python",
                    preferredQualifications="Testing",
                    bigClassificationName="Engineering",
                    middleClassificationName="Server",
                    detailClassificationName="Backend",
                    questions=[],
                )
            )

        failed_after = _sample_value("llm_request_duration_seconds_count", failed_labels)
        error_after = _sample_value("worker_llm_request_errors_total", error_labels)

        self.assertEqual(cm.exception.failure_reason, "VALIDATION_ERROR")
        self.assertEqual(failed_after, failed_before + 1.0)
        self.assertEqual(error_after, error_before + 1.0)

    def test_analysis_bad_request_status_error_is_non_retryable(self) -> None:
        worker = AnalysisOpenAiWorker.__new__(AnalysisOpenAiWorker)
        worker._task_type = "ANALYSIS"
        worker._model = "test-model"

        status_error = APIStatusError.__new__(APIStatusError)
        Exception.__init__(status_error, "bad request")
        status_error.status_code = 400
        status_error.response = SimpleNamespace(headers={})

        worker._client = types.SimpleNamespace(
            responses=types.SimpleNamespace(create=lambda **_kwargs: (_ for _ in ()).throw(status_error))
        )

        with self.assertRaises(NonRetryableWorkerError) as cm:
            worker.analyze(
                AnalysisWorkerContextResponse(
                    userId=1,
                    mockApplyId=1,
                    companyName="JobDri",
                    jobTitle="Backend Engineer",
                    task="Build APIs",
                    requirements="Python",
                    preferredQualifications="Testing",
                    bigClassificationName="Engineering",
                    middleClassificationName="Server",
                    detailClassificationName="Backend",
                    questions=[],
                )
            )

        self.assertEqual(cm.exception.failure_reason, "VALIDATION_ERROR")

    def test_analysis_validation_error_goes_to_non_retryable_path(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=MagicMock(),
            openai_worker=MagicMock(),
            analysis_openai_worker=MagicMock(),
            recovery_store=MagicMock(),
            terminal_message_store=MagicMock(),
            sleep_fn=lambda _seconds: None,
        )
        message = AnalysisTaskMessage(
            messageId="message-2",
            requestId="request-2",
            taskType="ANALYSIS",
            taskId="task-2",
            userId=1,
            mockApplyId=2,
            retryCount=0,
            maxRetryCount=3,
            submittedAt=datetime.fromisoformat("2026-07-21T00:00:00+00:00"),
        )
        method = SimpleNamespace(delivery_tag=2, redelivered=False)
        properties = SimpleNamespace(headers={})
        body = json.dumps(message.model_dump(mode="json")).encode("utf-8")

        with patch.object(
            consumer,
            "_process_analysis_task",
            side_effect=NonRetryableWorkerError("invalid response", failure_reason="VALIDATION_ERROR"),
        ), patch.object(
            consumer,
            "_handle_non_retryable",
            return_value="failed",
        ) as non_retryable_mock, patch.object(
            consumer,
            "_retry_or_fail",
        ) as retry_mock, patch.object(
            consumer,
            "_register_inflight",
            return_value=True,
        ), patch.object(
            consumer,
            "_release_inflight",
        ):
            consumer._on_message(channel=MagicMock(), method=method, properties=properties, body=body)

        non_retryable_mock.assert_called_once()
        retry_mock.assert_not_called()
        self.assertEqual(non_retryable_mock.call_args.args[-1].failure_reason, "VALIDATION_ERROR")


if __name__ == "__main__":
    unittest.main()
