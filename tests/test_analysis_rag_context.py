import unittest

from app.openai_prompts import (
    MAX_CORPUS_REFERENCE_CONTENT_LENGTH,
    MAX_CORPUS_REFERENCES_LENGTH,
    _build_corpus_reference_block,
    build_analysis_prompt,
)
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


def _corpus_reference(
    corpus_id: int = 11,
    category: str = "JOB_POSTING",
    rank: int = 1,
) -> dict[str, object]:
    return {
        "corpusId": corpus_id,
        "category": category,
        "title": "참고 회사 - 백엔드 개발자",
        "content": "주요 업무: API 개발\n자격 요건: Spring Boot",
        "rank": rank,
    }


class AnalysisRagContextTest(unittest.TestCase):

    def test_missing_similar_job_postings_defaults_to_empty_list(self) -> None:
        context = _base_context()

        self.assertEqual(context.corpusReferences, [])
        self.assertEqual(context.similarJobPostings, [])
        prompt = build_analysis_prompt(context)
        self.assertNotIn("[직무 평가 기준 (Curated Corpus)]", prompt)
        self.assertNotIn("[유사 채용공고 참고]", prompt)
        self.assertNotIn("[RAG Context 우선순위 및 사용 규칙]", prompt)

    def test_similar_job_postings_are_limited_to_top_three(self) -> None:
        context = _base_context(
            similarJobPostings=[
                _similar_job_posting(index)
                for index in [4, 2, 1, 3]
            ]
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
        self.assertIn("[유사 채용공고 참고]", prompt)
        self.assertIn("- 주요 업무: 유사 업무 1", prompt)
        self.assertIn("현재 분석 대상 채용공고가 항상 최우선 평가 기준이다", prompt)
        self.assertIn("현재 자기소개서 문항과 답변은 모든 참고 자료보다 우선", prompt)
        self.assertIn(
            "Curated Corpus나 Similar JobPosting에만 있는 요구사항을 현재 공고의 필수 조건",
            prompt,
        )
        self.assertIn("지원자의 경험, 성과, 역할 또는 계획을 추정하거나 만들어내지 않는다", prompt)

    def test_similar_job_posting_context_does_not_expose_vector_or_owner(self) -> None:
        similar_job_posting = _similar_job_posting(1)
        similar_job_posting["embedding"] = [0.1, 0.2]
        similar_job_posting["userId"] = 99
        context = _base_context(similarJobPostings=[similar_job_posting])
        serialized = context.model_dump(mode="json")["similarJobPostings"][0]

        self.assertNotIn("embedding", serialized)
        self.assertNotIn("userId", serialized)

    def test_corpus_references_are_parsed_and_rendered_as_job_evaluation_criteria(self) -> None:
        context = _base_context(corpusReferences=[_corpus_reference()])

        prompt = build_analysis_prompt(context)

        self.assertEqual(context.corpusReferences[0].corpusId, 11)
        self.assertIn("[직무 평가 기준 (Curated Corpus)]", prompt)
        self.assertIn("참고 회사 - 백엔드 개발자", prompt)
        self.assertIn("자격 요건: Spring Boot", prompt)
        self.assertIn("[RAG Context 우선순위 및 사용 규칙]", prompt)
        self.assertIn("현재 분석 대상 채용공고가 항상 최우선 평가 기준이다", prompt)

    def test_corpus_and_similar_job_postings_follow_prompt_priority(self) -> None:
        context = _base_context(
            corpusReferences=[_corpus_reference()],
            similarJobPostings=[_similar_job_posting(1)],
        )

        prompt = build_analysis_prompt(context)

        self.assertLess(prompt.index("[채용 공고]"), prompt.index("[직무 평가 기준 (Curated Corpus)]"))
        self.assertLess(
            prompt.index("[직무 평가 기준 (Curated Corpus)]"),
            prompt.index("[유사 채용공고 참고]"),
        )
        self.assertLess(prompt.index("[유사 채용공고 참고]"), prompt.index("[문항 및 답변]"))
        self.assertIn("현재 분석 대상 채용공고가 항상 최우선 평가 기준이다", prompt)
        self.assertIn("Curated Corpus와 Similar JobPosting이 충돌하면 Curated Corpus를 우선한다", prompt)
        self.assertIn(
            "현재 채용공고와 Curated Corpus 또는 Similar JobPosting이 충돌하면 현재 채용공고를 따른다",
            prompt,
        )
        self.assertIn(
            "Curated Corpus나 Similar JobPosting에만 있는 요구사항을 현재 공고의 필수 조건",
            prompt,
        )
        self.assertIn("지원자의 경험, 성과, 역할 또는 계획을 추정하거나 만들어내지 않는다", prompt)

    def test_corpus_references_are_ranked_and_limited_by_prompt_budget(self) -> None:
        references = []
        for rank in range(10, 0, -1):
            reference = _corpus_reference(corpus_id=100 + rank, rank=rank)
            reference["content"] = f"rank-{rank}-start|" + ("x" * 2_000) + f"|rank-{rank}-end"
            references.append(reference)
        context = _base_context(corpusReferences=references)

        block = _build_corpus_reference_block(context)
        reference_body = block.removeprefix("[직무 평가 기준 (Curated Corpus)]\n")

        self.assertLessEqual(len(reference_body), MAX_CORPUS_REFERENCES_LENGTH)
        self.assertLess(block.index("rank=1"), block.index("rank=2"))
        self.assertIn("rank-1-start", block)
        self.assertNotIn("rank-1-end", block)
        first_content = block.split("- 내용:\n", maxsplit=1)[1].split("\n\n", maxsplit=1)[0]
        self.assertEqual(len(first_content), MAX_CORPUS_REFERENCE_CONTENT_LENGTH)
        self.assertNotIn("rank=9", block)
        self.assertNotIn("rank=10", block)


if __name__ == "__main__":
    unittest.main()
