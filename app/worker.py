from __future__ import annotations

import logging
import signal
import threading

from app.consumer import RabbitMqConsumer
from app.logging_utils import bind_log_context, configure_worker_logging, log_info


configure_worker_logging()

logger = logging.getLogger(__name__)


def main() -> None:
    stop_event = threading.Event()
    consumer = RabbitMqConsumer()

    def handle_signal(signum, _frame) -> None:  # type: ignore[no-untyped-def]
        with bind_log_context():
            log_info(logger, "worker.process.signal", "종료 시그널을 수신했습니다.", signal=signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    with bind_log_context():
        log_info(logger, "worker.process.started", "Worker process started.")
    consumer.start()

    try:
        while not stop_event.wait(1):
            continue
    finally:
        consumer.stop()
        with bind_log_context():
            log_info(logger, "worker.process.stopped", "Worker process stopped.")


if __name__ == "__main__":
    main()
