from __future__ import annotations

from fastapi import FastAPI

from app.config import settings
from app.consumer import RabbitMqConsumer
from app.logging_utils import configure_worker_logging


configure_worker_logging()

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
