from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("APP_WORKER_INTERNAL_API_KEY", "test-internal-key")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

from pydantic import ValidationError

from app.async_runtime import AsyncConsumerRuntime
from app.concurrency import TaskTypeConcurrencyConfig, TaskTypeConcurrencyLimiter
from app.config import Settings
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

    def test_async_runtime_stop_waits_for_full_drain_timeout(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=MagicMock(),
            openai_worker=MagicMock(),
            analysis_openai_worker=MagicMock(),
            recovery_store=MagicMock(),
            terminal_message_store=MagicMock(),
            sleep_fn=lambda _seconds: None,
        )
        runtime = AsyncConsumerRuntime(consumer)
        runtime._loop = MagicMock()
        runtime._loop.is_running.return_value = True
        future = MagicMock()
        submitted_coro = None

        def fake_run_coroutine_threadsafe(coro, _loop):
            nonlocal submitted_coro
            submitted_coro = coro
            return future

        with patch("app.async_runtime.asyncio.run_coroutine_threadsafe", side_effect=fake_run_coroutine_threadsafe):
            runtime.stop()

        if submitted_coro is not None:
            submitted_coro.close()
        future.result.assert_called_once_with(timeout=35)

    def test_async_runtime_shutdown_once_deduplicates_cleanup(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=MagicMock(),
            openai_worker=MagicMock(),
            analysis_openai_worker=MagicMock(),
            recovery_store=MagicMock(),
            terminal_message_store=MagicMock(),
            sleep_fn=lambda _seconds: None,
        )
        runtime = AsyncConsumerRuntime(consumer)
        shutdown_calls = 0

        async def fake_shutdown() -> None:
            nonlocal shutdown_calls
            shutdown_calls += 1
            await asyncio.sleep(0)

        runtime._shutdown_async = fake_shutdown  # type: ignore[method-assign]

        async def run_once() -> None:
            await asyncio.gather(runtime._shutdown_once_async(), runtime._shutdown_once_async())

        asyncio.run(run_once())

        self.assertEqual(shutdown_calls, 1)

    def test_async_runtime_setup_failure_still_runs_shutdown(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=MagicMock(),
            openai_worker=MagicMock(),
            analysis_openai_worker=MagicMock(),
            recovery_store=MagicMock(),
            terminal_message_store=MagicMock(),
            sleep_fn=lambda _seconds: None,
        )
        runtime = AsyncConsumerRuntime(consumer)

        async def connect_robust(**_kwargs):
            raise RuntimeError("connect failed")

        aio_pika_stub = SimpleNamespace(connect_robust=connect_robust)

        async def run_once() -> None:
            with patch.dict(sys.modules, {"aio_pika": aio_pika_stub}), patch.object(
                runtime,
                "_shutdown_once_async",
                AsyncMock(),
            ) as shutdown_mock:
                with self.assertRaises(RuntimeError):
                    await runtime._consume_until_stopped()
                shutdown_mock.assert_awaited_once()

        asyncio.run(run_once())

    def test_async_runtime_waits_before_requeue_when_task_type_limit_is_reached(self) -> None:
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
        runtime = AsyncConsumerRuntime(consumer)
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
        incoming_message = SimpleNamespace(
            body=json.dumps(message.model_dump(mode="json")).encode("utf-8"),
            headers={},
            message_id=message.messageId,
            content_type="application/json",
            delivery_tag=1,
            redelivered=False,
            ack=AsyncMock(),
            nack=AsyncMock(),
        )

        async def fake_sleep(_seconds: float) -> None:
            return None

        with patch.object(consumer, "_register_inflight", return_value=True), patch.object(
            consumer,
            "_release_inflight",
        ) as release_mock, patch("app.async_runtime.asyncio.sleep", side_effect=fake_sleep) as sleep_mock:
            asyncio.run(runtime._handle_incoming_message(incoming_message))

        incoming_message.nack.assert_awaited_once_with(requeue=True)
        sleep_mock.assert_called_once()
        release_mock.assert_called_once_with("task-1")
        lease.release()

    def test_async_runtime_waits_before_requeue_when_task_is_already_inflight(self) -> None:
        consumer = RabbitMqConsumer(
            api_client=MagicMock(),
            openai_worker=MagicMock(),
            analysis_openai_worker=MagicMock(),
            recovery_store=MagicMock(),
            terminal_message_store=MagicMock(),
            sleep_fn=lambda _seconds: None,
        )
        runtime = AsyncConsumerRuntime(consumer)
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
        incoming_message = SimpleNamespace(
            body=json.dumps(message.model_dump(mode="json")).encode("utf-8"),
            headers={},
            message_id=message.messageId,
            content_type="application/json",
            delivery_tag=1,
            redelivered=True,
            ack=AsyncMock(),
            nack=AsyncMock(),
        )
        call_sequence: list[str] = []

        async def fake_sleep(_seconds: float) -> None:
            call_sequence.append("sleep")
            return None

        async def fake_nack(*, requeue: bool) -> None:
            call_sequence.append(f"nack:{requeue}")

        incoming_message.nack.side_effect = fake_nack

        with patch.object(consumer, "_register_inflight", return_value=False), patch(
            "app.async_runtime.asyncio.sleep",
            side_effect=fake_sleep,
        ) as sleep_mock:
            asyncio.run(runtime._handle_incoming_message(incoming_message))

        incoming_message.nack.assert_awaited_once_with(requeue=True)
        sleep_mock.assert_called_once()
        incoming_message.ack.assert_not_called()
        self.assertEqual(call_sequence, ["sleep", "nack:True"])

    def test_settings_reject_zero_concurrency_limit(self) -> None:
        with self.assertRaises(ValidationError):
            Settings(
                APP_WORKER_INTERNAL_API_KEY="test-internal-key",
                OPENAI_API_KEY="test-openai-key",
                WORKER_DEFAULT_CONCURRENCY_LIMIT=0,
            )

    def test_settings_reject_zero_prefetch_count(self) -> None:
        for invalid_value in (0, -1):
            with self.subTest(prefetch_count=invalid_value), self.assertRaises(ValidationError):
                Settings(
                    APP_WORKER_INTERNAL_API_KEY="test-internal-key",
                    OPENAI_API_KEY="test-openai-key",
                    WORKER_PREFETCH_COUNT=invalid_value,
                )


if __name__ == "__main__":
    unittest.main()
