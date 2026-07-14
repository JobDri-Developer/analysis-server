from __future__ import annotations

import logging

from fastapi import FastAPI

from app.config import settings
from app.consumer import RabbitMqConsumer


class WorkerContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for field_name in ("taskId", "workerId", "retryCount"):
            if not hasattr(record, field_name):
                setattr(record, field_name, "-")
        return True


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s %(levelname)s [%(name)s] "
        "[taskId=%(taskId)s workerId=%(workerId)s retryCount=%(retryCount)s] %(message)s"
    ),
)
logging.getLogger().addFilter(WorkerContextFilter())

app = FastAPI(title=settings.app_name)
consumer = RabbitMqConsumer()


@app.on_event("startup")
def startup_event() -> None:
    consumer.start()


@app.on_event("shutdown")
def shutdown_event() -> None:
    consumer.stop()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}
