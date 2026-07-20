from __future__ import annotations

import logging
import signal
import threading

from app.consumer import RabbitMqConsumer
from app.logging_utils import configure_worker_logging


configure_worker_logging()

logger = logging.getLogger(__name__)


def main() -> None:
    stop_event = threading.Event()
    consumer = RabbitMqConsumer()

    def handle_signal(signum, _frame) -> None:  # type: ignore[no-untyped-def]
        logger.info("종료 시그널을 수신했습니다. signal=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    consumer.start()
    logger.info("Worker process started.")

    try:
        while not stop_event.wait(1):
            continue
    finally:
        consumer.stop()
        logger.info("Worker process stopped.")


if __name__ == "__main__":
    main()
