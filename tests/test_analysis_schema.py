from __future__ import annotations

import unittest

from pydantic import ValidationError

from app.schemas import AnalysisLlmResponse


class AnalysisSchemaTests(unittest.TestCase):
    def _valid_payload(self) -> dict[str, object]:
        return {
            "jobFit": 80,
            "impact": 75,
            "completeness": 90,
            "feedback": "직무 관련 경험이 드러납니다.",
            "keyStrengths": [],
            "keyWeaknesses": [],
            "missingKeywords": [],
            "questionAnalyses": [
                {
                    "questionId": 1,
                    "sentence": "API 응답 시간을 단축했습니다.",
                    "status": "proven",
                    "reason": "행동과 결과가 구체적입니다.",
                    "improvement": None,
                }
            ],
        }

    def test_accepts_nullable_improvement_for_proven(self) -> None:
        response = AnalysisLlmResponse.model_validate(self._valid_payload())

        self.assertEqual(response.questionAnalyses[0].status, "proven")
        self.assertIsNone(response.questionAnalyses[0].improvement)

    def test_rejects_missing_status_and_unknown_fields(self) -> None:
        missing_status = self._valid_payload()
        del missing_status["questionAnalyses"][0]["status"]  # type: ignore[index]

        with self.assertRaises(ValidationError):
            AnalysisLlmResponse.model_validate(missing_status)

        unknown_field = self._valid_payload()
        unknown_field["unexpected"] = True

        with self.assertRaises(ValidationError):
            AnalysisLlmResponse.model_validate(unknown_field)

    def test_rejects_missing_and_unknown_question_statuses(self) -> None:
        for status in ("missing", "unsupported"):
            with self.subTest(status=status):
                payload = self._valid_payload()
                payload["questionAnalyses"][0]["status"] = status  # type: ignore[index]

                with self.assertRaises(ValidationError):
                    AnalysisLlmResponse.model_validate(payload)


if __name__ == "__main__":
    unittest.main()
