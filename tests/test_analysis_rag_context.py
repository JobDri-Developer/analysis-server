import unittest

from app.openai_prompts import build_analysis_prompt
from app.schemas import AnalysisWorkerContextResponse


def _base_context(**overrides: object) -> AnalysisWorkerContextResponse:
    data: dict[str, object] = {
        "userId": 1,
        "mockApplyId": 2,
        "companyName": "현재 회사",
        "jobTitle": "백엔드 개발자",
        "task": "현재 공고 API 개발",
        "requirements": "현재 공고 Spring Boot 경험",
        "preferredQualifications": "현재 공고 AWS 경험",
        "bigClassificationName": "개발",
        "middleClassificationName": "서버",
        "detailClassificationName": "백엔드",
        "questions": [],
    }
    data.update(overrides)
    return AnalysisWorkerContextResponse.model_validate(data)


def _similar_job_posting(index: int) -> dict[str, object]:
    return {
        "jobPostingId": 100 + index,
        "companyName": f"유사 회사 {index}",
        "postingName": f"유사 공고 {index}",
        "jobTitle": f"서버 개발자 {index}",
        "task": f"유사 업무 {index}",
        "requirements": f"유사 자격 {index}",
        "preferredQualifications": f"유사 우대 {index}",
        "similarityRank": index,
        "similarityScore": 1.0 - index / 10,
    }


class AnalysisRagContextTest(unittest.TestCase):

    def test_missing_similar_job_postings_defaults_to_empty_list(self) -> None:
        context = _base_context()

        self.assertEqual(context.similarJobPostings, [])
        self.assertNotIn("[유사 채용공고 참고 자료]", build_analysis_prompt(context))

    def test_similar_job_postings_are_limited_to_top_three(self) -> None:
        context = _base_context(
            similarJobPostings=[_similar_job_posting(index) for index in range(1, 5)]
        )

        self.assertEqual(
            [item.similarityRank for item in context.similarJobPostings],
            [1, 2, 3],
        )

    def test_prompt_separates_current_and_similar_job_postings_with_priority_rules(self) -> None:
        context = _base_context(similarJobPostings=[_similar_job_posting(1)])

        prompt = build_analysis_prompt(context)

        self.assertIn("[채용 공고]", prompt)
        self.assertIn("- 주요 업무: 현재 공고 API 개발", prompt)
        self.assertIn("[유사 채용공고 참고 자료]", prompt)
        self.assertIn("- 주요 업무: 유사 업무 1", prompt)
        self.assertIn("현재 분석 대상 채용공고가 항상 최우선 평가 기준이다", prompt)
        self.assertIn("현재 자기소개서 문항과 답변은 유사 채용공고보다 우선한다", prompt)
        self.assertIn("유사 채용공고에만 있는 요구사항을 현재 공고의 필수 조건", prompt)
        self.assertIn("지원자의 경험, 성과, 역할 또는 계획을 추정하거나 만들어내지 않는다", prompt)

    def test_similar_job_posting_context_does_not_expose_vector_or_owner(self) -> None:
        context = _base_context(similarJobPostings=[_similar_job_posting(1)])
        serialized = context.model_dump(mode="json")["similarJobPostings"][0]

        self.assertNotIn("embedding", serialized)
        self.assertNotIn("userId", serialized)


if __name__ == "__main__":
    unittest.main()
