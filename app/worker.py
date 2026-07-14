from __future__ import annotations

import logging
import signal
import threading

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
