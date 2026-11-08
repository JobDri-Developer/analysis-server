import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from app.openai_client import JobPostingOpenAiWorker
from app.openai_prompts import build_job_posting_extract_prompt
from app.openai_response_parser import parse_job_posting_extract_response
from app.schemas import JobPostingExtractResponse, JobPostingWorkerFinalizeRequest, JobPostingWorkerResultStoreRequest

FIXTURE = Path(__file__).parent / "fixtures" / "job-posting-worker-finalize.json"


def test_fields_survive_parsing_finalize_and_durable_result_round_trip():
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
    parsed = parse_job_posting_extract_response(json.dumps(fields))
    assert parsed.requiredSkills == []
    assert parsed.deadlineAt == ""
    assert parsed.model_dump()["requiredSkills"] == []
    other = JobPostingExtractResponse()
    parsed.requiredSkills.append("Java")
    assert other.requiredSkills == []


@pytest.mark.parametrize("async_mode", [False, True])
def test_sync_and_async_extraction_preserve_contract_without_network(async_mode):
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
    else:
        result = worker.extract("Java SQL", None)
        worker._create_response.assert_called_once()
    assert result.model_dump() == payload


def test_prompt_covers_skills_and_unambiguous_local_deadline_for_text_and_image():
    for has_image in [False, True]:
        prompt = build_job_posting_extract_prompt("test", has_image)
        assert '"requiredSkills": ["string"]' in prompt
        assert '"deadlineAt": "string"' in prompt
        assert "yyyy-MM-dd'T'HH:mm:ss" in prompt
        assert "임의로 만들지" in prompt
