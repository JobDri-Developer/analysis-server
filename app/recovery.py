from __future__ import annotations

import logging
import re
import threading
from pathlib import Path

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
                logger.exception("pending delivery spool 파일을 읽지 못했습니다. path=%s", path)
        return entries

    def _path_for(self, entry: PendingDeliveryEntry) -> Path:
        return self._path_for_task(entry.taskId, entry.deliveryKind)

    def _path_for_task(self, task_id: str, delivery_kind: str) -> Path:
        safe_task_id = re.sub(r"[^A-Za-z0-9._-]", "_", task_id)
        safe_kind = re.sub(r"[^A-Za-z0-9._-]", "_", delivery_kind.lower())
        return self._base_dir / f"{safe_kind}-{safe_task_id}.json"
