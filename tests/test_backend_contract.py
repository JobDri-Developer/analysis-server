"""Verify deployment and runtime gates reject incompatible backend schemas."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import requests

from app import backend_contract
from app.api_client import SpringWorkerApiClient
from app.config import settings
from app.schemas import RetryableWorkerError


def document():
    """Return a minimal compatible deployed OpenAPI schema."""
    return {"components": {"schemas": {"JobPostingExtractResponse": {"properties": {
        "requiredSkills": {"type": "array", "items": {"type": "string"}},
        "deadlineAt": {"type": "string"}}}}}}


def test_compatible_schema_and_rollback_are_detected(monkeypatch):
    """A formerly compatible backend must be rejected immediately after schema rollback."""
    current = document()
    response = SimpleNamespace(raise_for_status=lambda: None, json=lambda: current)
    monkeypatch.setattr(backend_contract.requests, "get", Mock(return_value=response))
    backend_contract.check_backend_contract()
    current["components"]["schemas"]["JobPostingExtractResponse"]["properties"].pop("deadlineAt")
    with pytest.raises(RetryableWorkerError):
        backend_contract.check_backend_contract()


def test_unavailable_schema_fails_closed(monkeypatch):
    """A failed compatibility check must not be treated as permission to deploy."""
    monkeypatch.setattr(backend_contract.requests, "get", Mock(side_effect=requests.ConnectionError()))
    with pytest.raises(RetryableWorkerError):
        backend_contract.check_backend_contract()


@pytest.mark.parametrize("async_mode", [False, True])
def test_production_requests_stop_before_sending_on_incompatible_backend(async_mode, monkeypatch):
    """Block both production transport paths before sending job-posting data."""
    monkeypatch.setattr(settings, "environment", "prod")
    client = object.__new__(SpringWorkerApiClient)
    client._send_request = Mock()
    client._send_request_async = AsyncMock()
    client._async_client = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(
        raise_for_status=lambda: None, json=lambda: {})))
    monkeypatch.setattr(backend_contract.requests, "get", Mock(return_value=SimpleNamespace(
        raise_for_status=lambda: None, json=lambda: {})))
    kwargs = dict(http_method="POST", path="/result", payload={}, task_type="JOB_POSTING_INGEST",
                  endpoint="job_posting_result", method="POST")
    with pytest.raises(RetryableWorkerError):
        if async_mode:
            asyncio.run(client._request_async(**kwargs))
        else:
            client._request(**kwargs)
    client._send_request.assert_not_called()
    client._send_request_async.assert_not_awaited()


def test_production_startup_checks_contract_before_consuming(monkeypatch):
    """An incompatible backend must prevent queue consumption at production startup."""
    from app import main

    monkeypatch.setattr(settings, "environment", "prod")
    start = Mock()
    monkeypatch.setattr(main.consumer, "start", start)
    monkeypatch.setattr(main, "check_backend_contract", Mock(side_effect=RetryableWorkerError("incompatible")))
    with pytest.raises(RetryableWorkerError):
        main.startup_event()
    start.assert_not_called()
