from __future__ import annotations

import json
import logging
import re
import threading
from pathlib import Path

from app.logging_utils import log_exception
from app.schemas import PendingDeliveryEntry

logger = logging.getLogger(__name__)


class PendingDeliveryStore:
    def __init__(self, base_dir: str | Path) -> None:
        self._base_dir = Path(base_dir)
        self._lock = threading.Lock()

    def upsert(self, entry: PendingDeliveryEntry) -> None:
        path = self._path_for(entry)
        serialized = entry.model_dump_json(indent=2)
        with self._lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = path.with_suffix(".tmp")
            temp_path.write_text(serialized, encoding="utf-8")
            temp_path.replace(path)

    def delete(self, task_id: str, delivery_kind: str) -> None:
        path = self._path_for_task(task_id, delivery_kind)
        with self._lock:
            if path.exists():
                path.unlink()

    def list_entries(self) -> list[PendingDeliveryEntry]:
        if not self._base_dir.exists():
            return []

        entries: list[PendingDeliveryEntry] = []
        with self._lock:
            paths = sorted(self._base_dir.glob("*.json"))

        for path in paths:
            try:
                entries.append(PendingDeliveryEntry.model_validate_json(path.read_text(encoding="utf-8")))
            except Exception:
                log_exception(
                    logger,
                    "worker.recovery.spool.read_failed",
                    "pending delivery spool 파일을 읽지 못했습니다.",
                    path=str(path),
                    errorCode="RECOVERY_SPOOL_READ_FAILED",
                )
        return entries

    def _path_for(self, entry: PendingDeliveryEntry) -> Path:
        return self._path_for_task(entry.taskId, entry.deliveryKind)

    def _path_for_task(self, task_id: str, delivery_kind: str) -> Path:
        safe_task_id = re.sub(r"[^A-Za-z0-9._-]", "_", task_id)
        safe_kind = re.sub(r"[^A-Za-z0-9._-]", "_", delivery_kind.lower())
        return self._base_dir / f"{safe_kind}-{safe_task_id}.json"


class TerminalMessageStore:
    def __init__(self, base_dir: str | Path) -> None:
        self._base_dir = Path(base_dir)
        self._lock = threading.Lock()

    def contains(self, task_id: str, message_id: str) -> bool:
        path = self._path_for(task_id, message_id)
        with self._lock:
            return path.exists()

    def record(self, *, task_id: str, message_id: str, task_type: str, failure_reason: str) -> None:
        path = self._path_for(task_id, message_id)
        payload = json.dumps(
            {
                "taskId": task_id,
                "messageId": message_id,
                "taskType": task_type,
                "failureReason": failure_reason,
            },
            ensure_ascii=True,
            indent=2,
        )
        with self._lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = path.with_suffix(".tmp")
            temp_path.write_text(payload, encoding="utf-8")
            temp_path.replace(path)

    def _path_for(self, task_id: str, message_id: str) -> Path:
        safe_task_id = re.sub(r"[^A-Za-z0-9._-]", "_", task_id)
        safe_message_id = re.sub(r"[^A-Za-z0-9._-]", "_", message_id)
        return self._base_dir / f"{safe_task_id}__{safe_message_id}.json"
