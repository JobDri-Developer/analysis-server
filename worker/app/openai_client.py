from __future__ import annotations

import json

from openai import OpenAI

from app.config import settings
from app.schemas import (
    JobPostingClassificationCandidateResponse,
    JobPostingClassificationResultResponse,
    JobPostingExtractResponse,
    JobPostingGenerateResponse,
)


class JobPostingOpenAiWorker:
    def __init__(self) -> None:
        self._client = OpenAI(api_key=settings.openai_api_key)
        self._model = settings.openai_job_posting_model

    def extract(self, raw_text: str | None, image_url: str | None) -> JobPostingExtractResponse:
        prompt = self._build_extract_prompt(raw_text or "", image_url is not None)
        content = [{"type": "input_text", "text": prompt}]
        if image_url:
            content.append({"type": "input_image", "image_url": image_url})

        try:
            response = self._client.responses.create(
                model=self._model,
                temperature=0.1,
                input=[{"role": "user", "content": content}],
            )
            payload = self._parse_json(response.output_text)
            return JobPostingExtractResponse.model_validate(payload)
        except Exception:
            return JobPostingExtractResponse(
                rawText=raw_text or "",
                confidence=0.0,
            )

    def classify(
        self,
        extracted: JobPostingExtractResponse,
        candidates: list[JobPostingClassificationCandidateResponse],
    ) -> JobPostingClassificationResultResponse:
        prompt = self._build_classification_prompt(extracted, candidates)
        try:
            response = self._client.responses.create(
                model=self._model,
                temperature=0.1,
                input=prompt,
            )
            payload = self._parse_json(response.output_text)
            return JobPostingClassificationResultResponse.model_validate(payload)
        except Exception:
            top = candidates[0]
            return JobPostingClassificationResultResponse(
                detailClassificationId=top.detailClassificationId,
                detailClassificationName=top.detailClassificationName,
                middleClassificationName=top.middleClassificationName,
                bigClassificationName=top.bigClassificationName,
                reason="LLM 분류 실패로 1순위 후보를 fallback으로 사용했습니다.",
                confidence=top.score,
            )

    def generate(
        self,
        extracted: JobPostingExtractResponse,
        classification: JobPostingClassificationResultResponse,
    ) -> JobPostingGenerateResponse:
        prompt = self._build_generation_prompt(extracted, classification)
        try:
            response = self._client.responses.create(
                model=self._model,
                temperature=0.7,
                input=prompt,
            )
            payload = self._parse_json(response.output_text)
            return JobPostingGenerateResponse.model_validate(payload)
        except Exception:
            return JobPostingGenerateResponse(
                companyName=extracted.companyName,
                jobTitle=extracted.jobTitle,
                task=extracted.task,
                requirements=extracted.requirements,
                preferredQualifications=extracted.preferredQualifications,
                summary="생성 실패로 추출 결과를 기반으로 fallback 응답을 사용했습니다.",
            )

    def _parse_json(self, raw_text: str) -> dict:
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        candidate = raw_text[start : end + 1] if start >= 0 and end >= 0 else raw_text
        return json.loads(candidate)

    def _build_extract_prompt(self, raw_text: str, has_image: bool) -> str:
        return f"""
이 {"이미지 또는 텍스트" if has_image else "텍스트"}는 채용 공고입니다.
회사명, 직무명, 주요 업무, 자격 요건, 우대 사항을 추출해주세요.
반드시 아래 JSON 형식만 반환하세요.

{{
  "companyName": "string",
  "jobTitle": "string",
  "task": "string",
  "requirements": "string",
  "preferredQualifications": "string",
  "rawText": "string",
  "confidence": 0.0
}}

[채용 공고 텍스트]
{raw_text}
""".strip()

    def _build_classification_prompt(
        self,
        extracted: JobPostingExtractResponse,
        candidates: list[JobPostingClassificationCandidateResponse],
    ) -> str:
        candidate_text = "\n".join(
            [
                (
                    f"- id={candidate.detailClassificationId} | 대분류={candidate.bigClassificationName} "
                    f"| 중분류={candidate.middleClassificationName} | 소분류={candidate.detailClassificationName} "
                    f"| score={candidate.score:.4f}"
                )
                for candidate in candidates
            ]
        )
        return f"""
다음 채용 공고 정보에 가장 적합한 소분류 후보를 하나 선택하세요.
반드시 JSON만 반환하세요.

{{
  "detailClassificationId": 1,
  "detailClassificationName": "string",
  "middleClassificationName": "string",
  "bigClassificationName": "string",
  "reason": "string",
  "confidence": 0.0
}}

[추출 결과]
- 회사명: {extracted.companyName}
- 직무명: {extracted.jobTitle}
- 주요 업무: {extracted.task}
- 자격 요건: {extracted.requirements}
- 우대 사항: {extracted.preferredQualifications}

[후보]
{candidate_text}
""".strip()

    def _build_generation_prompt(
        self,
        extracted: JobPostingExtractResponse,
        classification: JobPostingClassificationResultResponse,
    ) -> str:
        return f"""
다음 정보를 기반으로 저장 가능한 채용 공고 정제 결과를 JSON으로 생성하세요.
반드시 JSON만 반환하세요.

{{
  "companyName": "string",
  "jobTitle": "string",
  "task": "string",
  "requirements": "string",
  "preferredQualifications": "string",
  "summary": "string"
}}

[추출 결과]
- 회사명: {extracted.companyName}
- 직무명: {extracted.jobTitle}
- 주요 업무: {extracted.task}
- 자격 요건: {extracted.requirements}
- 우대 사항: {extracted.preferredQualifications}

[분류 결과]
- 대분류: {classification.bigClassificationName}
- 중분류: {classification.middleClassificationName}
- 소분류: {classification.detailClassificationName}
""".strip()
