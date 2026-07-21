from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

os.environ.setdefault("APP_WORKER_INTERNAL_API_KEY", "test-internal-key")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

if "requests" not in sys.modules:
    requests_stub = types.ModuleType("requests")

    class RequestException(Exception):
        pass

    class Session:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        def post(self, *args, **kwargs):
            raise NotImplementedError

        def get(self, *args, **kwargs):
            raise NotImplementedError

    requests_stub.RequestException = RequestException
    requests_stub.Session = Session
    sys.modules["requests"] = requests_stub

if "pika" not in sys.modules:
    pika_stub = types.ModuleType("pika")

    class BasicProperties:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class PlainCredentials:
        def __init__(self, username: str, password: str) -> None:
            self.username = username
            self.password = password

    class ConnectionParameters:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    class BlockingConnection:
        def __init__(self, parameters) -> None:
            self.parameters = parameters

    class BlockingChannel:
        pass

    pika_stub.BasicProperties = BasicProperties
    pika_stub.PlainCredentials = PlainCredentials
    pika_stub.ConnectionParameters = ConnectionParameters
    pika_stub.BlockingConnection = BlockingConnection
    pika_stub.adapters = types.SimpleNamespace(
        blocking_connection=types.SimpleNamespace(BlockingChannel=BlockingChannel)
    )
    sys.modules["pika"] = pika_stub

if "openai" not in sys.modules:
    openai_stub = types.ModuleType("openai")

    class OpenAiError(Exception):
        pass

    class APIConnectionError(OpenAiError):
        pass

    class APIStatusError(OpenAiError):
        status_code = None

    class APITimeoutError(OpenAiError):
        pass

    class BadRequestError(OpenAiError):
        pass

    class RateLimitError(OpenAiError):
        pass

    class OpenAI:
        def __init__(self, *args, **kwargs) -> None:
            self.responses = types.SimpleNamespace(create=lambda **_: None)

    openai_stub.APIConnectionError = APIConnectionError
    openai_stub.APIStatusError = APIStatusError
    openai_stub.APITimeoutError = APITimeoutError
    openai_stub.BadRequestError = BadRequestError
    openai_stub.OpenAI = OpenAI
    openai_stub.RateLimitError = RateLimitError
    sys.modules["openai"] = openai_stub

from app.api_client import SpringWorkerApiClient
from app.config import settings
from app.consumer import RabbitMqConsumer
from app.logging_utils import JsonLogFormatter, WorkerContextFilter, bind_log_context
from app.openai_client import AnalysisOpenAiWorker
from app.recovery import PendingDeliveryStore, TerminalMessageStore
from app.schemas import (
    AnalysisHighlightItem,
    AnalysisLlmResponse,
    AnalysisMissingKeywordItem,
    AnalysisQuestionAnalysisResponse,
    AnalysisTaskMessage,
    AnalysisTaskStatusResponse,
    AnalysisWorkerCompleteRequest,
    AnalysisQuestionContextResponse,
    AnalysisWorkerContextResponse,
    AnalysisWorkerFailureRequest,
    AnalysisWorkerResultStoreRequest,
    NonRetryableWorkerError,
    PendingDeliveryEntry,
    RetryableWorkerError,
)


class FakeApiClient:
    def __init__(
        self,
        *,
        complete_should_fail: bool = False,
        analysis_task_status: str | None = None,
    ) -> None:
        self.complete_should_fail = complete_should_fail
        self.analysis_task_status = analysis_task_status
        self.store_analysis_calls: list[tuple[str, AnalysisWorkerResultStoreRequest]] = []
        self.complete_analysis_calls: list[tuple[str, AnalysisWorkerCompleteRequest]] = []
        self.fail_analysis_calls: list[tuple[str, AnalysisWorkerFailureRequest]] = []

    def store_analysis_result(self, task_id: str, request: AnalysisWorkerResultStoreRequest) -> None:
        self.store_analysis_calls.append((task_id, request))

    def complete_analysis_task(self, task_id: str, request: AnalysisWorkerCompleteRequest) -> None:
        self.complete_analysis_calls.append((task_id, request))
        if self.complete_should_fail:
            raise RetryableWorkerError("complete timeout")

    def fail_analysis_task(self, task_id: str, request: AnalysisWorkerFailureRequest) -> None:
        self.fail_analysis_calls.append((task_id, request))
        self.analysis_task_status = "FAILED"

    def get_analysis_task(self, task_id: str) -> AnalysisTaskStatusResponse:
        return AnalysisTaskStatusResponse(
            status=self.analysis_task_status,
            failureReason="QUEUE_TIMEOUT" if self.analysis_task_status == "FAILED" else None,
        )


class FakeResponse:
    def __init__(self, status_code: int, payload: dict) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class FakeChannel:
    def __init__(self) -> None:
        self.acked_delivery_tags: list[int] = []
        self.nacked_delivery_tags: list[tuple[int, bool]] = []
        self.published_messages: list[dict[str, object]] = []

    def basic_ack(self, delivery_tag: int) -> None:
        self.acked_delivery_tags.append(delivery_tag)

    def basic_nack(self, delivery_tag: int, requeue: bool) -> None:
        self.nacked_delivery_tags.append((delivery_tag, requeue))

    def basic_publish(self, **kwargs):
        self.published_messages.append(kwargs)
        return True


class FakeMethod:
    def __init__(self, delivery_tag: int = 1, redelivered: bool = False) -> None:
        self.delivery_tag = delivery_tag
        self.redelivered = redelivered


class RecoveryFlowTests(unittest.TestCase):
    def _build_llm_response(self) -> AnalysisLlmResponse:
        return AnalysisLlmResponse(
            jobFit=80,
            impact=75,
            completeness=90,
            feedback="good",
            keyStrengths=[
                AnalysisHighlightItem(
                    title="구현 경험이 구체적으로 드러납니다.",
                    quote="answer",
                )
            ],
            keyWeaknesses=[
                AnalysisHighlightItem(
                    title="SQL 활용 경험 보강이 필요합니다.",
                    quote="SQL 활용 경험",
                )
            ],
            missingKeywords=[
                AnalysisMissingKeywordItem(
                    keyword="SQL 활용 경험",
                    source="qualification",
                )
            ],
            questionAnalyses=[
                AnalysisQuestionAnalysisResponse(
                    questionId=1,
                    sentence="answer",
                    status="mentioned",
                    reason="clear",
                    improvement="none",
                )
            ],
        )

    def _build_message(self) -> AnalysisTaskMessage:
        return AnalysisTaskMessage(
            messageId="m-1",
            requestId="req-1",
            taskType="ANALYSIS",
            taskId="task-1",
            userId=10,
            mockApplyId=20,
            retryCount=0,
            maxRetryCount=3,
            submittedAt="2026-07-19T00:00:00Z",
        )

    def test_store_success_then_complete_failure_keeps_pending_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api_client = FakeApiClient(complete_should_fail=True)
            store = PendingDeliveryStore(temp_dir)
            consumer = RabbitMqConsumer(
                api_client=api_client,
                openai_worker=object(),  # type: ignore[arg-type]
                analysis_openai_worker=object(),  # type: ignore[arg-type]
                recovery_store=store,
                sleep_fn=lambda _: None,
            )
            message = self._build_message()
            llm_response = self._build_llm_response()

            with patch.object(settings, "worker_api_retry_max_attempts", 1):
                consumer._store_analysis_result(message, llm_response)
                pending_entry = consumer._enqueue_pending_delivery(
                    message=message,
                    delivery_kind="ANALYSIS_COMPLETE",
                    delivery_path=f"/api/internal/worker/analysis/tasks/{message.taskId}/complete",
                    payload=consumer._build_analysis_complete_request(
                        message,
                        llm_response,
                        queue_latency_millis=123,
                    ).model_dump(mode="json"),
                    retry_count=message.retryCount,
                )
                delivered = consumer._deliver_pending_entry(
                    pending_entry,
                    retry_count=message.retryCount,
                    replayed=False,
                )

            self.assertFalse(delivered)
            self.assertEqual(len(api_client.store_analysis_calls), 1)
            self.assertEqual(len(api_client.complete_analysis_calls), 1)
            pending_entries = store.list_entries()
            self.assertEqual(len(pending_entries), 1)
            self.assertEqual(pending_entries[0].taskId, message.taskId)
            self.assertEqual(pending_entries[0].deliveryKind, "ANALYSIS_COMPLETE")
            self.assertEqual(pending_entries[0].requestId, message.requestId)
            self.assertEqual(pending_entries[0].messageId, message.messageId)
            self.assertEqual(pending_entries[0].taskType, message.taskType)

    def test_recovery_replays_pending_delivery_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = PendingDeliveryStore(temp_dir)
            message = self._build_message()
            llm_response = self._build_llm_response()
            request = AnalysisWorkerCompleteRequest(
                userId=message.userId,
                mockApplyId=message.mockApplyId,
                workerId="worker-1",
                queueLatencyMillis=321,
                llmResponse=llm_response,
            )
            store.upsert(
                PendingDeliveryEntry(
                    taskId=message.taskId,
                    requestId=message.requestId,
                    messageId=message.messageId,
                    taskType=message.taskType,
                    retryCount=message.retryCount,
                    deliveryKind="ANALYSIS_COMPLETE",
                    deliveryPath=f"/api/internal/worker/analysis/tasks/{message.taskId}/complete",
                    payload=request.model_dump(mode="json"),
                    storedAt="2026-07-19T00:00:00+00:00",
                )
            )

            api_client = FakeApiClient(complete_should_fail=False)
            consumer = RabbitMqConsumer(
                api_client=api_client,
                openai_worker=object(),  # type: ignore[arg-type]
                analysis_openai_worker=object(),  # type: ignore[arg-type]
                recovery_store=store,
                sleep_fn=lambda _: None,
            )

            with patch.object(settings, "worker_api_retry_max_attempts", 1):
                consumer._recover_pending_deliveries()

            self.assertEqual(len(api_client.complete_analysis_calls), 1)
            self.assertEqual(store.list_entries(), [])

    def test_analysis_complete_payload_contains_backend_contract_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            consumer = RabbitMqConsumer(
                api_client=FakeApiClient(),
                openai_worker=object(),  # type: ignore[arg-type]
                analysis_openai_worker=object(),  # type: ignore[arg-type]
                recovery_store=PendingDeliveryStore(temp_dir),
                sleep_fn=lambda _: None,
            )
            message = self._build_message()

            payload = consumer._build_analysis_complete_request(
                message,
                self._build_llm_response(),
                queue_latency_millis=123,
            ).model_dump(mode="json")

            llm_response = payload["llmResponse"]
            self.assertIn("keyStrengths", llm_response)
            self.assertIn("keyWeaknesses", llm_response)
            self.assertIn("missingKeywords", llm_response)
            self.assertEqual(llm_response["questionAnalyses"][0]["status"], "mentioned")

    def test_analysis_prompt_requires_question_analysis_for_every_answered_question(self) -> None:
        worker = AnalysisOpenAiWorker()
        prompt = worker._build_analysis_prompt(
            AnalysisWorkerContextResponse(
                userId=1,
                mockApplyId=2,
                companyName="잡드리",
                jobTitle="백엔드 개발자",
                task="API 개발",
                requirements="Spring Boot, REST API",
                preferredQualifications="테스트 코드 작성 경험",
                bigClassificationName="개발",
                middleClassificationName="서버",
                detailClassificationName="백엔드",
                questions=[
                    AnalysisQuestionContextResponse(
                        questionId=10,
                        question="지원 동기를 작성해주세요.",
                        answer="저는 Spring Boot API 개발 경험을 바탕으로 지원했습니다.",
                        charLimit=700,
                    ),
                    AnalysisQuestionContextResponse(
                        questionId=11,
                        question="문제 해결 경험을 작성해주세요.",
                        answer="장애 로그를 분석하고 재시도 로직을 개선했습니다.",
                        charLimit=700,
                    ),
                    AnalysisQuestionContextResponse(
                        questionId=12,
                        question="추가로 강조하고 싶은 내용을 작성해주세요.",
                        answer="",
                        charLimit=700,
                    ),
                ],
            )
        )

        self.assertIn("모든 입력 문항은 questionAnalyses에 최소 1개 이상 포함한다", prompt)
        self.assertIn("모든 questionId를 빠짐없이 커버해야 한다", prompt)
        self.assertIn("문항당 최대 2개까지 포함한다", prompt)
        self.assertIn('"status": "proven|mentioned|fabricated"', prompt)
        self.assertIn("status는 proven, mentioned, fabricated 중 하나만 사용한다", prompt)
        self.assertNotIn('"status": "proven|mentioned|missing|fabricated"', prompt)
        self.assertNotIn("status는 proven, mentioned, missing, fabricated 중 하나만 사용한다", prompt)
        self.assertIn("questionAnalyses에는 사용하지 말고 missingKeywords와 keyWeaknesses로만 표현한다", prompt)
        self.assertIn("questionId=12\n  question=추가로 강조하고 싶은 내용을 작성해주세요.\n  answer=\n  charLimit=700", prompt)

    def test_analysis_usage_fields_include_token_counts(self) -> None:
        worker = AnalysisOpenAiWorker()
        response = types.SimpleNamespace(
            usage=types.SimpleNamespace(
                input_tokens=12000,
                output_tokens=1500,
                total_tokens=13500,
                input_tokens_details=types.SimpleNamespace(cached_tokens=1024),
                output_tokens_details=types.SimpleNamespace(reasoning_tokens=0),
            )
        )

        usage_fields = worker._extract_usage_fields(response)

        self.assertEqual(
            usage_fields,
            {
                "inputTokens": 12000,
                "outputTokens": 1500,
                "totalTokens": 13500,
                "cachedInputTokens": 1024,
                "reasoningOutputTokens": 0,
            },
        )

    def test_analysis_usage_fields_support_dict_response(self) -> None:
        worker = AnalysisOpenAiWorker()
        response = types.SimpleNamespace(
            usage={
                "input_tokens": 9000,
                "output_tokens": 1000,
                "total_tokens": 10000,
                "input_tokens_details": {"cached_tokens": 512},
            }
        )

        usage_fields = worker._extract_usage_fields(response)

        self.assertEqual(
            usage_fields,
            {
                "inputTokens": 9000,
                "outputTokens": 1000,
                "totalTokens": 10000,
                "cachedInputTokens": 512,
            },
        )

    def test_store_analysis_result_treats_conflict_as_success(self) -> None:
        client = SpringWorkerApiClient()
        client._session.post = lambda *args, **kwargs: FakeResponse(
            409,
            {
                "isSuccess": False,
                "code": "ALREADY_EXISTS",
                "message": "already stored",
                "result": None,
                "error": "conflict",
            },
        )

        request = AnalysisWorkerResultStoreRequest(
            userId=1,
            mockApplyId=2,
            llmResponse=self._build_llm_response(),
        )

        client.store_analysis_result("task-1", request)

    def test_analysis_task_message_accepts_epoch_submitted_at(self) -> None:
        message = AnalysisTaskMessage.model_validate(
            {
                "messageId": "m-1",
                "taskType": "ANALYSIS",
                "taskId": "task-1",
                "userId": 10,
                "mockApplyId": 20,
                "retryCount": 0,
                "maxRetryCount": 3,
                "submittedAt": 1784534106.0190554,
            }
        )

        self.assertIsNotNone(message.submittedAt.tzinfo)
        self.assertEqual(message.model_dump(mode="json")["submittedAt"], "2026-07-20T07:55:06.019055Z")

    def test_analysis_task_message_accepts_iso_submitted_at(self) -> None:
        message = AnalysisTaskMessage.model_validate(
            {
                "messageId": "m-1",
                "taskType": "ANALYSIS",
                "taskId": "task-1",
                "userId": 10,
                "mockApplyId": 20,
                "retryCount": 0,
                "maxRetryCount": 3,
                "submittedAt": "2026-07-19T00:00:00Z",
            }
        )

        self.assertIsNotNone(message.submittedAt.tzinfo)
        self.assertEqual(message.model_dump(mode="json")["submittedAt"], "2026-07-19T00:00:00Z")

    def test_analysis_task_message_accepts_int_epoch_submitted_at(self) -> None:
        message = AnalysisTaskMessage.model_validate(
            {
                "messageId": "m-1",
                "taskType": "ANALYSIS",
                "taskId": "task-1",
                "userId": 10,
                "mockApplyId": 20,
                "retryCount": 0,
                "maxRetryCount": 3,
                "submittedAt": 1784534106,
            }
        )

        self.assertIsNotNone(message.submittedAt.tzinfo)
        self.assertEqual(message.model_dump(mode="json")["submittedAt"], "2026-07-20T07:55:06Z")

    def test_worker_context_filter_sets_defaults_for_missing_fields(self) -> None:
        log_record = logging.LogRecord(
            name="pika.adapters.blocking_connection",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="connection open",
            args=(),
            exc_info=None,
        )

        self.assertTrue(WorkerContextFilter().filter(log_record))
        self.assertIsNone(log_record.taskId)
        self.assertIsNone(log_record.messageId)
        self.assertIsNone(log_record.workerId)
        self.assertIsNone(log_record.retryCount)
        self.assertEqual(log_record.logType, "application")

    def test_json_log_formatter_does_not_mask_token_usage_metrics(self) -> None:
        log_record = logging.LogRecord(
            name="app.openai_client",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="OpenAI analysis 호출이 완료되었습니다.",
            args=(),
            exc_info=None,
        )
        log_record.event = "openai.generate.completed"
        log_record.inputTokens = 12000
        log_record.outputTokens = 1500
        log_record.totalTokens = 13500
        log_record.cachedInputTokens = 1024
        log_record.reasoningOutputTokens = 0
        log_record.openai_api_key = "sk-test"

        payload = json.loads(JsonLogFormatter().format(log_record))

        self.assertEqual(payload["inputTokens"], 12000)
        self.assertEqual(payload["outputTokens"], 1500)
        self.assertEqual(payload["totalTokens"], 13500)
        self.assertEqual(payload["cachedInputTokens"], 1024)
        self.assertEqual(payload["reasoningOutputTokens"], 0)
        self.assertEqual(payload["openai_api_key"], "***masked***")

    def test_deserialize_message_reads_headers(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=FakeApiClient(),
            openai_worker=object(),  # type: ignore[arg-type]
            analysis_openai_worker=object(),  # type: ignore[arg-type]
            sleep_fn=lambda _: None,
        )
        properties = sys.modules["pika"].BasicProperties(
            headers={
                "x-request-id": "req-header",
                "x-task-id": "task-header",
                "x-task-type": "ANALYSIS",
                "x-retry-count": 2,
                "x-message-id": "msg-header",
            }
        )
        payload = {
            "messageId": "m-1",
            "taskType": "ANALYSIS",
            "taskId": "task-1",
            "userId": 10,
            "mockApplyId": 20,
            "retryCount": 0,
            "maxRetryCount": 3,
            "submittedAt": "2026-07-19T00:00:00Z",
        }

        message = consumer._deserialize_message(payload, properties)

        self.assertEqual(message.requestId, "req-header")
        self.assertEqual(message.taskId, "task-header")
        self.assertEqual(message.taskType, "ANALYSIS")
        self.assertEqual(message.retryCount, 2)
        self.assertEqual(message.messageId, "msg-header")

    def test_deserialize_message_generates_request_id_when_missing(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=FakeApiClient(),
            openai_worker=object(),  # type: ignore[arg-type]
            analysis_openai_worker=object(),  # type: ignore[arg-type]
            sleep_fn=lambda _: None,
        )
        payload = {
            "messageId": "m-1",
            "taskType": "ANALYSIS",
            "taskId": "task-1",
            "userId": 10,
            "mockApplyId": 20,
            "retryCount": 0,
            "maxRetryCount": 3,
            "submittedAt": "2026-07-19T00:00:00Z",
        }

        message = consumer._deserialize_message(payload, None)

        self.assertIsNotNone(message.requestId)
        self.assertTrue(message.requestId.startswith("worker-"))

    def test_api_client_forwards_request_id_header(self) -> None:
        client = SpringWorkerApiClient()
        captured_headers: dict[str, str] = {}

        def fake_post(*args, **kwargs):
            captured_headers.update(kwargs.get("headers", {}))
            return FakeResponse(
                200,
                {
                    "isSuccess": True,
                    "code": "OK",
                    "message": "ok",
                    "result": None,
                    "error": None,
                },
            )

        client._session.post = fake_post

        with bind_log_context(requestId="req-forward"):
            client.store_analysis_result(
                "task-1",
                AnalysisWorkerResultStoreRequest(
                    userId=1,
                    mockApplyId=2,
                    llmResponse=self._build_llm_response(),
                ),
            )

        self.assertEqual(captured_headers["X-Request-Id"], "req-forward")

    def test_on_message_acks_invalid_payload_after_deserialization_failure(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=FakeApiClient(),
            openai_worker=object(),  # type: ignore[arg-type]
            analysis_openai_worker=object(),  # type: ignore[arg-type]
            sleep_fn=lambda _: None,
        )
        channel = FakeChannel()
        method = FakeMethod(delivery_tag=99)
        invalid_message_body = json.dumps(
            {
                "messageId": "m-1",
                "taskType": "ANALYSIS",
                "taskId": "task-1",
                "userId": 10,
                "mockApplyId": 20,
                "retryCount": 0,
                "maxRetryCount": 3,
                "submittedAt": {"invalid": True},
            }
        ).encode("utf-8")

        consumer._on_message(channel, method, None, invalid_message_body)

        self.assertEqual(channel.acked_delivery_tags, [99])

    def test_queue_timeout_publishes_dlq_only_once_for_same_message(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api_client = FakeApiClient()
            consumer = RabbitMqConsumer(
                api_client=api_client,
                openai_worker=object(),  # type: ignore[arg-type]
                analysis_openai_worker=object(),  # type: ignore[arg-type]
                terminal_message_store=TerminalMessageStore(temp_dir),
                sleep_fn=lambda _: None,
            )
            consumer._compute_queue_latency_millis = lambda _: 999_999  # type: ignore[method-assign]
            message = self._build_message()
            body = json.dumps(message.model_dump(mode="json")).encode("utf-8")
            channel = FakeChannel()

            with patch.object(settings, "analysis_queue_timeout_millis", 1):
                consumer._on_message(channel, FakeMethod(delivery_tag=1), None, body)
                consumer._on_message(channel, FakeMethod(delivery_tag=2), None, body)

            self.assertEqual(channel.acked_delivery_tags, [1, 2])
            self.assertEqual(channel.nacked_delivery_tags, [])
            self.assertEqual(len(channel.published_messages), 1)
            self.assertEqual(len(api_client.fail_analysis_calls), 1)
            self.assertEqual(channel.published_messages[0]["routing_key"], settings.analysis_rabbitmq_dlq)

    def test_queue_timeout_skips_dlq_when_task_is_already_failed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api_client = FakeApiClient(analysis_task_status="FAILED")
            consumer = RabbitMqConsumer(
                api_client=api_client,
                openai_worker=object(),  # type: ignore[arg-type]
                analysis_openai_worker=object(),  # type: ignore[arg-type]
                terminal_message_store=TerminalMessageStore(temp_dir),
                sleep_fn=lambda _: None,
            )
            consumer._compute_queue_latency_millis = lambda _: 999_999  # type: ignore[method-assign]
            message = self._build_message()
            body = json.dumps(message.model_dump(mode="json")).encode("utf-8")
            channel = FakeChannel()

            with patch.object(settings, "analysis_queue_timeout_millis", 1):
                consumer._on_message(channel, FakeMethod(delivery_tag=7), None, body)

            self.assertEqual(channel.acked_delivery_tags, [7])
            self.assertEqual(channel.nacked_delivery_tags, [])
            self.assertEqual(channel.published_messages, [])
            self.assertEqual(api_client.fail_analysis_calls, [])

    def test_non_retryable_failure_does_not_requeue_when_dlq_publish_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api_client = FakeApiClient()
            consumer = RabbitMqConsumer(
                api_client=api_client,
                openai_worker=object(),  # type: ignore[arg-type]
                analysis_openai_worker=object(),  # type: ignore[arg-type]
                terminal_message_store=TerminalMessageStore(temp_dir),
                sleep_fn=lambda _: None,
            )
            message = self._build_message()
            channel = FakeChannel()

            def publish_fail(*args, **kwargs) -> bool:
                return False

            consumer._publish_dlq = publish_fail  # type: ignore[method-assign]
            consumer._handle_non_retryable(
                channel,
                delivery_tag=11,
                message=message,
                body=json.dumps(message.model_dump(mode="json")).encode("utf-8"),
                properties=None,
                error=NonRetryableWorkerError(
                    "이미 처리되었거나 중복된 요청입니다.",
                    failure_reason="VALIDATION_ERROR",
                ),
            )

            self.assertEqual(channel.acked_delivery_tags, [11])
            self.assertEqual(channel.nacked_delivery_tags, [])
            self.assertEqual(api_client.fail_analysis_calls[0][0], message.taskId)

    def test_terminal_task_is_acked_even_when_dlq_publish_failed_previously(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api_client = FakeApiClient(analysis_task_status="FAILED")
            consumer = RabbitMqConsumer(
                api_client=api_client,
                openai_worker=object(),  # type: ignore[arg-type]
                analysis_openai_worker=object(),  # type: ignore[arg-type]
                terminal_message_store=TerminalMessageStore(temp_dir),
                sleep_fn=lambda _: None,
            )
            channel = FakeChannel()
            message = self._build_message()
            body = json.dumps(message.model_dump(mode="json")).encode("utf-8")

            consumer._handle_non_retryable(
                channel,
                delivery_tag=12,
                message=message,
                body=body,
                properties=None,
                error=NonRetryableWorkerError(
                    "이미 처리되었거나 중복된 요청입니다.",
                    failure_reason="VALIDATION_ERROR",
                ),
            )

            self.assertEqual(channel.acked_delivery_tags, [12])
            self.assertEqual(channel.nacked_delivery_tags, [])
            self.assertEqual(channel.published_messages, [])
            self.assertEqual(api_client.fail_analysis_calls, [])


if __name__ == "__main__":
    unittest.main()
