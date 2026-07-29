from __future__ import annotations

from app.schemas import (
    AnalysisWorkerContextResponse,
    JobPostingClassificationCandidateResponse,
    JobPostingClassificationResultResponse,
    JobPostingExtractResponse,
)


def build_job_posting_extract_prompt(raw_text: str, has_image: bool) -> str:
    return f"""
이 {"이미지 또는 텍스트" if has_image else "텍스트"}는 채용 공고입니다.
공고 제목, 회사명, 직무명, 주요 업무, 자격 요건, 우대 사항을 추출해주세요.
반드시 아래 JSON 형식만 반환하세요.

{{
  "postingName": "string",
  "companyName": "string",
  "jobTitle": "string",
  "task": "string",
  "requirements": "string",
  "preferredQualifications": "string",
  "rawText": "string",
  "confidence": 0.0
}}

규칙:
1. postingName은 원문에 명시된 채용 공고 제목을 그대로 추출하세요.
2. 원문에 공고 제목이 없거나 확실하지 않으면 postingName은 반드시 빈 문자열로 두세요.
3. postingName을 회사명과 직무명으로 새로 만들거나 요약하지 마세요.

[채용 공고 텍스트]
{raw_text}
""".strip()


def build_job_posting_classification_prompt(
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


def build_job_posting_generation_prompt(
    extracted: JobPostingExtractResponse,
    classification: JobPostingClassificationResultResponse,
) -> str:
    return f"""
다음 정보를 기반으로 저장 가능한 채용 공고 정제 결과를 JSON으로 생성하세요.
반드시 JSON만 반환하세요.

{{
  "postingName": "string",
  "companyName": "string",
  "jobTitle": "string",
  "task": "string",
  "requirements": "string",
  "preferredQualifications": "string",
  "summary": "string"
}}

[추출 결과]
- 공고명: {extracted.postingName}
- 회사명: {extracted.companyName}
- 직무명: {extracted.jobTitle}
- 주요 업무: {extracted.task}
- 자격 요건: {extracted.requirements}
- 우대 사항: {extracted.preferredQualifications}

규칙:
1. postingName은 추출 결과의 공고명을 그대로 사용하세요.
2. 추출된 공고명이 빈 문자열이면 postingName을 새로 만들지 말고 빈 문자열로 두세요.

[분류 결과]
- 대분류: {classification.bigClassificationName}
- 중분류: {classification.middleClassificationName}
- 소분류: {classification.detailClassificationName}
""".strip()


def build_analysis_prompt(context: AnalysisWorkerContextResponse) -> str:
    question_block = "\n".join(
        [
            (
                f"- questionId={question.questionId}\n"
                f"  question={question.question}\n"
                f"  answer={question.answer}\n"
                f"  charLimit={question.charLimit}"
            )
            for question in context.questions
        ]
    )
    similar_job_posting_block = _build_similar_job_posting_block(context)
    return f"""
당신은 자기소개서 분석 평가자입니다.
지원 직무 적합도, 답변의 임팩트, 전체 완성도를 0부터 100 사이 정수로 평가하고,
전체 피드백, 핵심 강점/약점, 누락 키워드, 각 문항별 분석을 JSON으로만 반환하세요.

반드시 아래 스키마만 반환하세요.
{{
  "jobFit": 0,
  "impact": 0,
  "completeness": 0,
  "feedback": "string",
  "keyStrengths": [
    {{
      "title": "짧은 핵심 강점 문장",
      "quote": "자소서 답변에 실제 포함된 정확한 부분 문자열"
    }}
  ],
  "keyWeaknesses": [
    {{
      "title": "짧은 핵심 약점 문장",
      "quote": "JD 또는 자소서 답변에 실제 포함된 정확한 부분 문자열"
    }}
  ],
  "missingKeywords": [
    {{
      "keyword": "JD에는 있지만 답변에서 충분히 드러나지 않은 짧은 역량/요건",
      "source": "qualification|preference|mainTask"
    }}
  ],
  "questionAnalyses": [
    {{
      "questionId": 1,
      "sentence": "string",
      "status": "proven|mentioned|fabricated",
      "reason": "string",
      "improvement": "string"
    }}
  ]
}}

[판정 규칙]
- jobFit, impact, completeness는 0부터 100 사이 정수만 사용한다.
- questionAnalyses의 questionId는 입력된 questionId 중 하나만 사용한다.
- questionAnalyses의 sentence는 반드시 해당 questionId의 answer에 실제 포함된 정확한 substring이어야 한다.
- answer가 비어 있지 않은 모든 입력 문항은 questionAnalyses에 최소 1개 이상 포함한다.
- questionAnalyses는 비어 있지 않은 answer를 가진 모든 questionId를 빠짐없이 커버해야 한다.
- 각 문항에서 가장 평가 가치가 큰 실제 문장 1개를 우선 선택하고, 필요하면 문항당 최대 2개까지 포함한다.
- 강한 긍정 근거가 부족한 문항도 생략하지 말고, 해당 answer의 실제 문장 1개를 골라 mentioned 또는 fabricated로 평가한다.
- 원문 매칭이 불확실하면 문장을 요약하거나 재작성하지 말고, 해당 answer에서 더 짧고 정확히 일치하는 substring을 다시 선택한다.
- status는 proven, mentioned, fabricated 중 하나만 사용한다.
- proven: 답변에 구체적인 근거, 행동, 결과가 충분히 드러남
- mentioned: 관련 키워드나 경험은 있으나 구체적인 근거, 에피소드, 결과가 부족함
- fabricated: 답변에 없는 내용을 있는 것처럼 주장하거나 과장 위험이 큼
- 관련 언급이 전혀 없는 missing 사례는 원문 sentence가 없으므로 questionAnalyses에는 사용하지 말고 missingKeywords와 keyWeaknesses로만 표현한다.
- keyStrengths와 keyWeaknesses는 각각 최대 3개이며, 없으면 []로 출력한다.
- keyStrengths의 quote는 자소서 answer에 실제 포함된 substring만 사용한다.
- missingKeywords는 최대 3개이며, 없으면 []로 출력한다.
- missingKeywords의 source는 qualification, preference, mainTask 중 하나만 사용한다.
- keyWeaknesses의 첫 항목들은 가능하면 missingKeywords와 같은 누락 요건을 다룬다.
- missingKeywords 기반 keyWeaknesses의 quote는 JD의 주요 업무, 자격 요건, 우대 사항에 실제 포함된 표현을 사용한다.
- missingKeywords가 없으면 keyWeaknesses는 questionAnalyses의 보완 대상 문장 quote를 우선 사용한다.
- 모든 title은 한 문장으로 짧게 작성한다.

[채용 공고]
- 회사명: {context.companyName}
- 직무명: {context.jobTitle}
- 주요 업무: {context.task}
- 자격 요건: {context.requirements}
- 우대 사항: {context.preferredQualifications}
- 직무 분류: {context.bigClassificationName} > {context.middleClassificationName} > {context.detailClassificationName}

[문항 및 답변]
{question_block}
{similar_job_posting_block}
""".strip()


def _build_similar_job_posting_block(context: AnalysisWorkerContextResponse) -> str:
    similar_job_postings = context.similarJobPostings[:3]
    if not similar_job_postings:
        return ""

    references = "\n\n".join(
        (
            f"{item.similarityRank}.\n"
            f"- 회사: {item.companyName}\n"
            f"- 공고명: {item.postingName}\n"
            f"- 직무: {item.jobTitle}\n"
            f"- 주요 업무: {item.task}\n"
            f"- 자격 요건: {item.requirements}\n"
            f"- 우대 사항: {item.preferredQualifications}"
        )
        for item in similar_job_postings
    )
    return f"""

[유사 채용공고 참고 자료]
{references}

[유사 채용공고 사용 규칙]
- 현재 분석 대상 채용공고가 항상 최우선 평가 기준이다.
- 현재 자기소개서 문항과 답변은 유사 채용공고보다 우선한다.
- 유사 채용공고는 실제 공고 표현과 요구 역량을 이해하기 위한 보조 참고 자료일 뿐이다.
- 현재 채용공고와 유사 채용공고가 충돌하면 현재 채용공고를 따른다.
- 유사 채용공고에만 있는 요구사항을 현재 공고의 필수 조건, 누락 키워드 또는 감점 근거로 사용하지 않는다.
- 유사 채용공고를 근거로 지원자의 경험, 성과, 역할 또는 계획을 추정하거나 만들어내지 않는다.
- 자기소개서 원문에 없는 사실을 improvement에 추가하지 않는다.
""".rstrip()
