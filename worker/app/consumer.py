from __future__ import annotations

import json
import logging
import threading
from typing import Any

import pika

from app.api_client import SpringWorkerApiClient
from app.config import settings
from app.openai_client import JobPostingOpenAiWorker
from app.schemas import (
    JobPostingClassificationResultResponse,
    JobPostingIngestResponse,
    JobPostingIngestTaskMessage,
    JobPostingWorkerFinalizeRequest,
    NonRetryableWorkerError,
    RetryableWorkerError,
)

logger = logging.getLogger(__name__)


class RabbitMqConsumer:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None
        self._api_client = SpringWorkerApiClient()
        self._openai_worker = JobPostingOpenAiWorker()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="rabbitmq-consumer", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._channel and self._channel.is_open:
            try:
                self._channel.stop_consuming()
            except Exception:
                logger.exception("RabbitMQ consuming stop 중 오류가 발생했습니다.")
        if self._connection and self._connection.is_open:
            self._connection.close()

    def _run(self) -> None:
        credentials = pika.PlainCredentials(settings.rabbitmq_username, settings.rabbitmq_password)
        parameters = pika.ConnectionParameters(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            virtual_host=settings.rabbitmq_vhost,
            credentials=credentials,
            heartbeat=30,
        )

        while not self._stop_event.is_set():
            try:
                self._connection = pika.BlockingConnection(parameters)
                self._channel = self._connection.channel()
                self._channel.basic_qos(prefetch_count=settings.rabbitmq_prefetch_count)
                self._channel.basic_consume(
                    queue=settings.rabbitmq_queue,
                    on_message_callback=self._on_message,
                )
                logger.info("RabbitMQ consumer started. queue=%s", settings.rabbitmq_queue)
                self._channel.start_consuming()
            except Exception:
                logger.exception("RabbitMQ consumer 연결/소비 중 오류가 발생했습니다.")
                if self._stop_event.wait(5):
                    break

    def _on_message(self, channel, method, properties, body: bytes) -> None:  # type: ignore[no-untyped-def]
        try:
            payload = json.loads(body.decode("utf-8"))
            message = JobPostingIngestTaskMessage.model_validate(payload)
        except Exception:
            logger.exception("메시지 역직렬화에 실패했습니다.")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            self._process_job_posting_task(message)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except NonRetryableWorkerError as exc:
            logger.warning("비재시도 에러로 작업을 실패 처리합니다. taskId=%s error=%s", message.taskId, exc)
            self._safe_fail_task(message.taskId, str(exc))
            self._publish_dlq(body, properties)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except RetryableWorkerError as exc:
            logger.warning("재시도 가능한 에러가 발생했습니다. taskId=%s error=%s", message.taskId, exc)
            self._retry_or_fail(channel, method.delivery_tag, properties, message, body, str(exc))
        except Exception as exc:
            logger.exception("예상치 못한 worker 에러가 발생했습니다. taskId=%s", message.taskId)
            self._retry_or_fail(channel, method.delivery_tag, properties, message, body, str(exc))

    def _process_job_posting_task(self, message: JobPostingIngestTaskMessage) -> None:
        if message.taskType != "JOB_POSTING_INGEST":
            raise NonRetryableWorkerError(f"지원하지 않는 taskType입니다. taskType={message.taskType}")

        self._api_client.mark_running(message.taskId)

        context = self._api_client.get_context(message.userId, message.imageObjectKey)
        extracted = self._openai_worker.extract(message.rawText, context.imageUrl)
        candidates = self._api_client.get_candidates(extracted)
        if not candidates:
            raise NonRetryableWorkerError("소분류 후보를 찾을 수 없습니다.")

        classification = self._openai_worker.classify(extracted, candidates)
        classification = self._normalize_classification(classification, candidates)

        if classification.confidence < settings.job_posting_confidence_threshold:
            result = JobPostingIngestResponse(
                savedToDatabase=False,
                message="소분류 분류 confidence가 낮아 저장을 보류했습니다.",
                extracted=extracted,
                candidates=candidates,
                classification=classification,
                generated=None,
                saved=None,
            )
            self._api_client.complete_task(message.taskId, result)
            return

        generated = self._openai_worker.generate(extracted, classification)
        self._api_client.finalize(
            JobPostingWorkerFinalizeRequest(
                taskId=message.taskId,
                userId=message.userId,
                extracted=extracted,
                candidates=candidates,
                classification=classification,
                generated=generated,
            )
        )

    def _normalize_classification(
        self,
        classification: JobPostingClassificationResultResponse,
        candidates,
    ) -> JobPostingClassificationResultResponse:
        for candidate in candidates:
            if candidate.detailClassificationId == classification.detailClassificationId:
                return JobPostingClassificationResultResponse(
                    detailClassificationId=candidate.detailClassificationId,
                    detailClassificationName=candidate.detailClassificationName,
                    middleClassificationName=candidate.middleClassificationName,
                    bigClassificationName=candidate.bigClassificationName,
                    reason=classification.reason,
                    confidence=classification.confidence,
                )
        top = candidates[0]
        return JobPostingClassificationResultResponse(
            detailClassificationId=top.detailClassificationId,
            detailClassificationName=top.detailClassificationName,
            middleClassificationName=top.middleClassificationName,
            bigClassificationName=top.bigClassificationName,
            reason=classification.reason or "분류 결과를 후보와 정규화하는 과정에서 1순위 후보를 사용했습니다.",
            confidence=classification.confidence,
        )

    def _retry_or_fail(self, channel, delivery_tag: int, properties, message, body: bytes, error_message: str) -> None:
        next_retry_count = message.retryCount + 1
        if next_retry_count > settings.worker_max_retry_count:
            self._safe_fail_task(message.taskId, error_message)
            self._publish_dlq(body, properties)
            channel.basic_ack(delivery_tag=delivery_tag)
            return

        republished = message.model_copy(update={"retryCount": next_retry_count}).model_dump(mode="json")
        channel.basic_publish(
            exchange=settings.rabbitmq_exchange,
            routing_key=settings.rabbitmq_routing_key,
            body=json.dumps(republished, ensure_ascii=True),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                headers={"x-retry-count": next_retry_count},
            ),
        )
        channel.basic_ack(delivery_tag=delivery_tag)

    def _publish_dlq(self, body: bytes, properties: Any) -> None:
        if not self._channel or not self._channel.is_open:
            return
        self._channel.basic_publish(
            exchange="",
            routing_key=settings.rabbitmq_dlq,
            body=body,
            properties=pika.BasicProperties(
                content_type=getattr(properties, "content_type", "application/json"),
                delivery_mode=2,
                headers=getattr(properties, "headers", None),
            ),
        )

    def _safe_fail_task(self, task_id: str, error_message: str) -> None:
        try:
            self._api_client.fail_task(task_id, error_message)
        except Exception:
            logger.exception("Spring API에 실패 상태를 반영하지 못했습니다. taskId=%s", task_id)
