from __future__ import annotations

import logging


class WorkerContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for field_name in ("taskId", "messageId", "workerId", "retryCount"):
            if not hasattr(record, field_name):
                setattr(record, field_name, "-")
        return True


def configure_worker_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s [%(name)s] "
            "[taskId=%(taskId)s messageId=%(messageId)s workerId=%(workerId)s retryCount=%(retryCount)s] %(message)s"
        ),
    )

    root_logger = logging.getLogger()
    context_filter = WorkerContextFilter()
    root_logger.addFilter(context_filter)
    for handler in root_logger.handlers:
        handler.addFilter(context_filter)
