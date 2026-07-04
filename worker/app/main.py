from __future__ import annotations

import logging

from fastapi import FastAPI

from app.config import settings
from app.consumer import RabbitMqConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

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
