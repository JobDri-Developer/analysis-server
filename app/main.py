from __future__ import annotations

import logging

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.config import settings
from app.consumer import RabbitMqConsumer
from app.logging_utils import bind_log_context, configure_worker_logging, log_info


configure_worker_logging()

app = FastAPI(title=settings.app_name)
consumer = RabbitMqConsumer()
logger = logging.getLogger(__name__)


@app.on_event("startup")
def startup_event() -> None:
    with bind_log_context(logType=settings.worker_log_type):
        log_info(logger, "worker.app.startup", "FastAPI worker startup을 시작합니다.")
    consumer.start()


@app.on_event("shutdown")
def shutdown_event() -> None:
    with bind_log_context(logType=settings.worker_log_type):
        log_info(logger, "worker.app.shutdown", "FastAPI worker shutdown을 시작합니다.")
    consumer.stop()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
