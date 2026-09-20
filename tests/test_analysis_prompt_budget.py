import unittest
from unittest.mock import patch

from pydantic import ValidationError

from app.config import settings
from app.error_codes import FailureReasonCode
from app.openai_client import AnalysisOpenAiWorker
from app.schemas import (
    AnalysisWorkerContextResponse,
    MAX_FEW_SHOT_PROMPT_BLOCK_LENGTH,
    NonRetryableWorkerError,
)


def _context(*, prompt_block: str | None = None) -> AnalysisWorkerContextResponse:
    data: dict[str, object] = {
        "userId": 1,
        "mockApplyId": 2,
        "companyName": "현재 회사",
        "jobTitle": "백엔드 개발자",
        "task": "API 개발",
        "requirements": "Spring Boot",
        "preferredQualifications": "AWS",
        "bigClassificationName": "개발",
        "middleClassificationName": "서버",
        "detailClassificationName": "백엔드",
        "questions": [],
    }
    if prompt_block is not None:
        data["fewShot"] = {
            "promptBlock": prompt_block,
            "selectionMetadata": {
                "selectionMode": "EMBEDDING",
                "reason": "",
                "datasetVersion": "fewshot-v1",
                "minSimilarity": 0.4,
                "topK": 5,
                "minimumSelectedCount": 2,
                "scoreType": "COSINE_SIMILARITY",
                "cohereApiCallCount": 1,
                "selectedCases": [],
            },
        }
    return AnalysisWorkerContextResponse.model_validate(data)


class AnalysisPromptBudgetTest(unittest.TestCase):

    def test_schema_rejects_few_shot_block_over_backend_contract_limit(self) -> None:
        with self.assertRaises(ValidationError):
            _context(prompt_block="x" * (MAX_FEW_SHOT_PROMPT_BLOCK_LENGTH + 1))

    def test_worker_rejects_total_prompt_over_configured_budget_before_request(self) -> None:
        worker = AnalysisOpenAiWorker.__new__(AnalysisOpenAiWorker)

        with patch.object(settings, "analysis_prompt_max_chars", 100):
            with self.assertRaises(NonRetryableWorkerError) as captured:
                worker._build_analysis_prompt(_context(prompt_block="승인된 예시"))

        self.assertEqual(
            captured.exception.failure_reason,
            FailureReasonCode.VALIDATION_ERROR.value,
        )

    def test_worker_accepts_prompt_within_configured_budget(self) -> None:
        worker = AnalysisOpenAiWorker.__new__(AnalysisOpenAiWorker)

        with patch.object(settings, "analysis_prompt_max_chars", 120_000):
            prompt = worker._build_analysis_prompt(_context(prompt_block="승인된 예시"))

        self.assertIn("[Few-shot 예시]", prompt)


if __name__ == "__main__":
    unittest.main()
