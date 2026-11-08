"""Fail closed when the deployed backend cannot accept the worker extraction contract."""

from typing import Any

import requests

from app.config import settings
from app.schemas import RetryableWorkerError


def validate_backend_contract(document: dict[str, Any]) -> None:
    """Require the deployed OpenAPI model to expose both extraction fields and types."""
    properties = document.get("components", {}).get("schemas", {}).get("JobPostingExtractResponse", {}).get("properties", {})
    skills = properties.get("requiredSkills", {})
    deadline = properties.get("deadlineAt", {})
    if (skills.get("type") != "array" or skills.get("items", {}).get("type") != "string"
            or deadline.get("type") != "string"):
        raise RetryableWorkerError("Backend extraction contract is incompatible; deploy a compatible backend first.")


def check_backend_contract() -> None:
    """Check the live backend schema without reading or modifying application data."""
    try:
        response = requests.get(
            settings.spring_api_base_url.rstrip("/") + "/v3/api-docs",
            headers={"X-Internal-Api-Key": settings.spring_internal_api_key}, timeout=10, allow_redirects=False,
        )
        response.raise_for_status()
        validate_backend_contract(response.json())
    except (requests.RequestException, ValueError, AttributeError, TypeError) as exc:
        raise RetryableWorkerError("Cannot verify backend extraction compatibility; refusing job-posting requests.") from exc


async def check_backend_contract_async(client: Any) -> None:
    """Check the live schema through the existing asynchronous HTTP client."""
    import httpx

    try:
        response = await client.get("/v3/api-docs")
        response.raise_for_status()
        validate_backend_contract(response.json())
    except (httpx.HTTPError, ValueError, AttributeError, TypeError) as exc:
        raise RetryableWorkerError("Cannot verify backend extraction compatibility; refusing job-posting requests.") from exc
