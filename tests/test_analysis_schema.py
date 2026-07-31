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

    def test_missing_keyword_source_allows_only_main_task_and_qualification(self) -> None:
        for source in ("mainTask", "qualification"):
            with self.subTest(source=source):
                payload = self._valid_payload()
                payload["missingKeywords"] = [{"keyword": "API 개발", "source": source}]

                response = AnalysisLlmResponse.model_validate(payload)

                self.assertEqual(response.missingKeywords[0].source, source)

        for source in ("preference", "unknown"):
            with self.subTest(source=source):
                payload = self._valid_payload()
                payload["missingKeywords"] = [{"keyword": "API 개발", "source": source}]

                with self.assertRaises(ValidationError):
                    AnalysisLlmResponse.model_validate(payload)

    def test_score_fields_enforce_inclusive_zero_to_one_hundred_range(self) -> None:
        for field_name in ("jobFit", "impact", "completeness"):
            for invalid_score in (-1, 101):
                with self.subTest(field=field_name, score=invalid_score):
                    payload = self._valid_payload()
                    payload[field_name] = invalid_score

                    with self.assertRaises(ValidationError):
                        AnalysisLlmResponse.model_validate(payload)

        schema = AnalysisLlmResponse.model_json_schema()
        for field_name in ("jobFit", "impact", "completeness"):
            self.assertEqual(schema["properties"][field_name]["minimum"], 0)
            self.assertEqual(schema["properties"][field_name]["maximum"], 100)


if __name__ == "__main__":
    unittest.main()
