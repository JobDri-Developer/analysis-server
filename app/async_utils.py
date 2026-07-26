from __future__ import annotations

import inspect
from typing import Any


async def await_if_needed(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
