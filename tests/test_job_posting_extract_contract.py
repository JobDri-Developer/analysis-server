import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from app.api_client import SpringWorkerApiClient

import pytest

from app.openai_client import JobPostingOpenAiWorker
from app.openai_prompts import build_job_posting_extract_prompt
from app.openai_response_parser import parse_job_posting_extract_response
from app.schemas import JobPostingExtractResponse, JobPostingWorkerFinalizeRequest, JobPostingWorkerResultStoreRequest

FIXTURE = Path(__file__).parent / "fixtures" / "job-posting-worker-finalize.json"


def test_fields_survive_parsing_finalize_and_durable_result_round_trip():
    """Verify extraction fields survive model and durable-result serialization."""
    payload = json.loads(FIXTURE.read_text())
    parsed = parse_job_posting_extract_response(json.dumps(payload["extracted"]))
    assert parsed.requiredSkills == ["Java", "SQL"]
    assert parsed.deadlineAt == "2026-11-30T18:00:00"
    finalize = JobPostingWorkerFinalizeRequest.model_validate(payload)
    stored = JobPostingWorkerResultStoreRequest(userId=1, result=finalize)
    restored = JobPostingWorkerResultStoreRequest.model_validate_json(stored.model_dump_json())
    assert restored.result.model_dump(mode="json") == payload


@pytest.mark.parametrize("fields", [{}, {"requiredSkills": None, "deadlineAt": None}])
def test_legacy_and_null_fields_use_java_compatible_defaults(fields):
    """Verify omitted and null fields use independent Java-compatible defaults."""
    parsed = parse_job_posting_extract_response(json.dumps(fields))
    assert parsed.requiredSkills == []
    assert parsed.deadlineAt == ""
    assert parsed.model_dump()["requiredSkills"] == []
    other = JobPostingExtractResponse()
    parsed.requiredSkills.append("Java")
    assert other.requiredSkills == []


@pytest.mark.parametrize("async_mode", [False, True])
def test_sync_and_async_extraction_preserve_contract_without_network(async_mode):
    """Inspect the prompt sent by each extraction path and preserve response checks."""
    payload = json.loads(FIXTURE.read_text())["extracted"]
    worker = object.__new__(JobPostingOpenAiWorker)
    worker._task_type = "JOB_POSTING_INGEST"
    worker._model = "test-model"
    response = SimpleNamespace(output_text=json.dumps(payload), id="test-response")
    worker._create_response = Mock(return_value=response)
    worker._create_response_async = AsyncMock(return_value=response)
    if async_mode:
        result = asyncio.run(worker.extract_async("Java SQL", None))
        worker._create_response_async.assert_awaited_once()
        sent = worker._create_response_async.call_args.kwargs
    else:
        result = worker.extract("Java SQL", None)
        worker._create_response.assert_called_once()
        sent = worker._create_response.call_args.kwargs
    prompt = sent["input_payload"][0]["content"][0]["text"]
    assert '"requiredSkills": ["string"]' in prompt
    assert '"deadlineAt": "string"' in prompt
    assert "yyyy-MM-dd'T'HH:mm:ss" in prompt
    assert result.model_dump() == payload


def test_prompt_covers_skills_and_unambiguous_local_deadline_for_text_and_image():
    """Verify both input modes describe the shared extraction contract."""
    for has_image in [False, True]:
        prompt = build_job_posting_extract_prompt("test", has_image)
        assert '"requiredSkills": ["string"]' in prompt
        assert '"deadlineAt": "string"' in prompt
        assert "yyyy-MM-dd'T'HH:mm:ss" in prompt
        assert "임의로 만들지" in prompt


def test_required_skills_deduplicate_after_validation_without_reordering():
    """Keep first occurrences and reject invalid values before deduplication."""
    from pydantic import ValidationError

    result = JobPostingExtractResponse(requiredSkills=["SQL", "Java", "SQL", "Python", "Java"])
    assert result.requiredSkills == ["SQL", "Java", "Python"]
    assert JobPostingExtractResponse(requiredSkills=[]).requiredSkills == []
    for skills in [[None], [123], [["Java"]]]:
        with pytest.raises(ValidationError):
            JobPostingExtractResponse(requiredSkills=skills)


@pytest.mark.parametrize("async_mode", [False, True])
def test_api_store_request_body_and_backend_readback_preserve_fields(async_mode, monkeypatch):
    """Exercise actual client POST/GET serialization against an in-memory HTTP backend mock."""
    from app.config import settings

    monkeypatch.setattr(settings, "environment", "local")
    client = object.__new__(SpringWorkerApiClient)
    saved = {}
    payload = json.loads(FIXTURE.read_text())
    request = JobPostingWorkerResultStoreRequest(
        userId=1, result=JobPostingWorkerFinalizeRequest.model_validate(payload))

    def post(url, *, json, **kwargs):
        """Capture the JSON body at the HTTP boundary as a simulated backend write."""
        assert url.endswith("/tasks/contract-task/result")
        assert json["result"]["extracted"]["requiredSkills"] == ["Java", "SQL"]
        assert json["result"]["extracted"]["deadlineAt"] == "2026-11-30T18:00:00"
        saved.update(json["result"])
        return response(None)

    def get(url, **kwargs):
        """Return the stored backend envelope rather than the original model object."""
        assert url.endswith("/tasks/contract-task/result")
        return response({"taskId": "contract-task", "resultPayload": json.dumps(saved)})

    def response(result):
        """Build the Spring API envelope consumed by the real client response parser."""
        return SimpleNamespace(status_code=200, json=lambda: {
            "isSuccess": True, "code": "COMMON200", "message": "ok", "result": result})

    client._session = SimpleNamespace(post=post, get=get)
    client._async_client = SimpleNamespace(post=AsyncMock(side_effect=post), get=AsyncMock(side_effect=get))
    if async_mode:
        async def round_trip():
            """Await the independent asynchronous store and readback methods."""
            await client.store_job_posting_result_async("contract-task", request)
            return await client.get_job_posting_result_async("contract-task")
        restored = asyncio.run(round_trip())
        client._async_client.post.assert_awaited_once()
        client._async_client.get.assert_awaited_once()
    else:
        client.store_job_posting_result("contract-task", request)
        restored = client.get_job_posting_result("contract-task")
    assert restored.model_dump(mode="json") == payload
