from __future__ import annotations

import json

from app.schemas import (
    AnalysisQuestionAnalysisResponse,
    AnalysisWorkerContextResponse,
    JobPostingClassificationCandidateResponse,
    JobPostingClassificationResultResponse,
    JobPostingExtractResponse,
)

MAX_CORPUS_REFERENCE_ITEMS = 8
MAX_CORPUS_REFERENCE_CONTENT_LENGTH = 1_200
MAX_CORPUS_REFERENCES_LENGTH = 6_000


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
    corpus_reference_block = _build_corpus_reference_block(context)
    similar_job_posting_block = _build_similar_job_posting_block(context)
    rag_priority_block = _build_rag_priority_block(context)
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
      "source": "qualification|mainTask"
    }}
  ],
  "questionAnalyses": [
    {{
      "questionId": 1,
      "sentence": "string",
      "status": "proven|mentioned|fabricated",
      "reason": "string",
      "improvement": null
    }}
  ]
}}

[판정 규칙]
- jobFit, impact, completeness는 0부터 100 사이 정수만 사용한다.
- jobFit은 JD의 핵심 업무·필수요건 대부분을 직접 증명해야 85 이상, 주요 요건을 증명하되 일부 핵심 요건이 누락되면 70~84, 일부만 증명하면 55~69로 평가한다.
- impact는 주요 주장 대부분에 구체적인 행동·결과가 있어야 70 이상이며, 관련 경험을 일반적으로 언급한 문장이 섞이면 그 비중을 반영한다.
- completeness는 질문 적합성, 논리 흐름, 표현의 일관성을 평가한다. 동일 프로젝트의 기간·인원·역할이 직접 충돌하면 완성도에 실질적으로 반영한다.
- 일부 좋은 문장만 보고 전체 점수를 정하지 말고 비어 있지 않은 모든 문항과 누락 요건, 내부 모순을 함께 반영한다.
- 수치가 없다는 이유만으로 감점하지 않으며, 구체적인 행동과 결과가 있으면 수치 없이도 proven이 될 수 있다.
- 포부와 향후 계획은 과거 성과 수치를 요구하지 않고 실행 대상, 방법, 직무 연결성으로 평가한다.
- questionAnalyses의 questionId는 입력된 questionId 중 하나만 사용한다.
- questionAnalyses의 sentence는 반드시 해당 questionId의 answer에 실제 포함된 정확한 substring이어야 한다.
- answer가 비어 있지 않고 서로 다른 평가 문장이 2개 이상인 모든 입력 문항은 questionAnalyses에 정확히 2개 포함한다.
- 유효한 평가 문장이 1개뿐인 문항만 예외적으로 1개를 반환한다.
- questionAnalyses는 비어 있지 않은 answer를 가진 모든 questionId를 빠짐없이 커버해야 한다.
- 각 문항에서 서로 다른 평가 관점을 보여 주는 실제 문장을 문항당 최대 2개까지 포함한다.
- 문장 선택 전에 같은 answer 안에서 동일한 프로젝트·경력·성과를 가리키는 기간, 인원, 역할, 수치가 함께 성립할 수 있는지 교차 확인한다.
- 동일 대상을 설명하는 두 진술이 직접 충돌하면 일반적인 proven 문장보다 fabricated 문장을 우선해 반드시 포함한다.
- 하나의 직접 충돌을 이루는 두 문장을 각각 fabricated로 중복 반환하지 말고, 충돌을 가장 분명히 보여 주는 문장 하나만 대표로 선택한다.
- 동일 questionId에서는 fabricated를 최대 1개만 반환한다.
- 각 문항의 대표 문장은 근거 수준과 사실 정합성에 따라 proven, mentioned 또는 fabricated로 평가한다.
- 원문 매칭이 불확실하면 문장을 요약하거나 재작성하지 말고, 해당 answer에서 더 짧고 정확히 일치하는 substring을 다시 선택한다.
- status는 proven, mentioned, fabricated 중 하나만 사용한다.
- proven: 답변에 구체적인 근거, 행동, 결과가 충분히 드러남
- mentioned: 관련 키워드나 경험은 있으나 구체적인 근거, 에피소드, 결과가 부족함
- fabricated: JD 또는 답변 내부의 명시적 사실과 직접 충돌하거나, 하지 않았다고 밝힌 경험을 했다고 주장함
- 단순한 근거 부족, 수치 부족, 과장 가능성만으로 fabricated를 사용하지 않고 mentioned를 사용한다.
- fabricated의 reason에는 어떤 두 사실이 "직접 충돌합니다"라고 명시한다.
- proven의 improvement는 null로 반환한다.
- mentioned 또는 fabricated는 같은 answer에 있는 사실과 표현만 사용해 바로 교체 가능한 완성 문장을 우선 작성한다.
- mentioned 또는 fabricated의 improvement는 새 수치·경력·역할을 만들지 않고도 안전하게 개선할 수 있으면 반드시 문자열로 반환한다.
- 같은 answer의 사실만으로도 안전한 대체 문장을 만들 수 없는 경우에만 improvement를 null로 반환한다.
- improvement에 "추가하면 좋습니다", "수정할 수 있습니다", "보완해야 합니다", "강조하는 방향" 같은 첨삭 조언을 쓰지 않는다.
- improvement는 원문 대신 바로 붙여 넣을 수 있도록 지원자의 행동이나 계획을 직접 서술한다.
- 관련 언급이 전혀 없는 missing 사례는 원문 sentence가 없으므로 questionAnalyses에는 사용하지 말고 missingKeywords와 keyWeaknesses로만 표현한다.
- keyStrengths와 keyWeaknesses는 각각 최대 3개이며, 없으면 []로 출력한다.
- keyStrengths의 quote는 자소서 answer에 실제 포함된 substring만 사용한다.
- missingKeywords는 최대 3개이며, 없으면 []로 출력한다.
- missingKeywords는 mainTask 또는 qualification에 있는 핵심 경험형 요건만 사용하고, preference에만 있는 항목은 제외한다.
- 같은 요건이 mainTask와 preference에 모두 있으면 source는 mainTask를 사용한다.
- missingKeywords의 source는 mainTask 또는 qualification만 사용한다.
- JD의 한 문구에 여러 개념이 결합돼 있어도 답변이 그 핵심 행동을 실질적으로 다루면, 일부 단어가 없다는 이유로 전체 문구를 missing으로 판정하지 않는다.
- missingKeywords를 확정하기 전에 비어 있지 않은 모든 답변을 다시 확인하고, 동일 키워드뿐 아니라 명확한 동의 표현과 실제 수행 행동도 언급으로 인정한다.
- keyWeaknesses의 첫 항목들은 가능하면 missingKeywords와 같은 누락 요건을 다룬다.
- missingKeywords 기반 keyWeaknesses의 quote는 JD의 주요 업무, 자격 요건, 우대 사항에 실제 포함된 표현을 사용한다.
- missingKeywords가 없으면 keyWeaknesses는 questionAnalyses의 보완 대상 문장 quote를 우선 사용한다.
- 모든 title은 한 문장으로 짧게 작성한다.

[상태 판정 예시]
- proven 예시: "로그를 분석해 재시도 정책을 수정했고 오류율을 4%에서 1%로 낮췄습니다."처럼 행동과 결과가 구체적인 문장
- mentioned 예시: "고객 데이터를 활용해 성과를 개선했습니다."처럼 관련 경험은 있지만 대상, 방법, 결과가 부족한 문장
- fabricated 예시: 같은 프로젝트를 앞에서는 "2개월 개인 프로젝트"라고 하고 뒤에서는 "5개월간 4명이 수행한 팀 프로젝트"라고 한 경우. 두 진술이 직접 충돌한다고 설명한다.
- missing 예시: JD의 mainTask에 "재고 예측 모델 운영"이 있으나 모든 답변에 관련 언급이 없다면 questionAnalyses가 아니라 missingKeywords에 넣는다.
- 예시는 의미 기준만 보여 주며, 실제 입력에 없는 문장이나 상태를 만들기 위해 복사하지 않는다.

[채용 공고]
- 회사명: {context.companyName}
- 직무명: {context.jobTitle}
- 주요 업무: {context.task}
- 자격 요건: {context.requirements}
- 우대 사항: {context.preferredQualifications}
- 직무 분류: {context.bigClassificationName} > {context.middleClassificationName} > {context.detailClassificationName}

{corpus_reference_block}
{similar_job_posting_block}
{rag_priority_block}

[문항 및 답변]
{question_block}
""".strip()


def build_analysis_question_analyses_recovery_prompt(
    context: AnalysisWorkerContextResponse,
    question_ids: list[int],
    current_analyses: list[AnalysisQuestionAnalysisResponse],
) -> str:
    selected_question_ids = set(question_ids)
    selected_questions = [
        question for question in context.questions if question.questionId in selected_question_ids
    ]
    question_block = "\n\n".join(
        (
            f"- questionId={question.questionId}\n"
            f"  question={question.question}\n"
            f"  answer={question.answer}\n"
            f"  charLimit={question.charLimit}"
        )
        for question in selected_questions
    )
    existing_block = json.dumps(
        [
            item.model_dump(mode="json")
            for item in current_analyses
            if item.questionId in selected_question_ids
        ],
        ensure_ascii=False,
        indent=2,
    )
    return f"""
당신은 자기소개서 문항별 분석 복구 평가자입니다.
아래 선택 문항만 다시 검토하여 questionAnalyses를 JSON으로 반환하세요.

반드시 아래 스키마만 반환하세요.
{{
  "questionAnalyses": [
    {{
      "questionId": 1,
      "sentence": "해당 answer에 실제 포함된 정확한 부분 문자열",
      "status": "proven|mentioned|fabricated",
      "reason": "판정 근거",
      "improvement": null
    }}
  ]
}}

[복구 규칙]
- 선택된 각 문항의 answer에 서로 다른 평가 문장이 2개 이상이면 정확히 2개를 반환한다.
- 유효한 평가 문장이 1개뿐인 경우에만 1개를 반환한다.
- 선택되지 않은 questionId는 절대 반환하지 않는다.
- sentence는 반드시 같은 questionId의 answer에 실제 포함된 정확한 substring이어야 한다.
- 같은 문장을 중복 반환하지 않는다.
- 기존 분석은 유지하거나 더 정확한 status, reason, improvement로 교체할 수 있다.
- status는 proven, mentioned, fabricated 중 하나만 사용한다.
- proven은 구체적인 행동·근거·결과가 충분한 문장이며 improvement는 null이다.
- mentioned는 관련 내용은 있으나 대상·방법·근거·결과가 부족한 문장이다.
- fabricated는 답변 내부의 명시적 사실이 서로 직접 충돌하거나 하지 않은 일을 했다고 주장한 문장이다.
- fabricated의 reason에는 충돌하는 두 사실을 밝히고 반드시 "직접 충돌합니다"라는 표현을 포함한다.
- 단순히 수치나 설명이 부족한 문장을 fabricated로 판정하지 않는다.
- mentioned 또는 fabricated는 같은 answer에 이미 있는 사실만 사용해 바로 교체 가능한 완성 문장을 우선 작성한다.
- 새 수치·기간·인원·역할·경험을 만들지 않고 안전하게 개선할 수 있으면 improvement를 반드시 문자열로 반환한다.
- 같은 answer의 사실만으로 안전한 대체 문장을 만들 수 없는 경우에만 improvement를 null로 반환한다.
- improvement는 첨삭 조언이나 설명이 아니라 사용자가 그대로 바꿔 쓸 수 있는 자기소개서 문장이어야 한다.
- improvement에 "추가하면 좋습니다", "수정할 수 있습니다", "보완해야 합니다", "강조하는 방향" 같은 첨삭 조언을 쓰지 않는다.
- improvement는 "저는", "프로젝트에서", "입사 후"처럼 지원자의 행동이나 계획을 직접 서술하는 형태로 작성한다.

[직무 정보]
- 회사명: {context.companyName}
- 직무명: {context.jobTitle}
- 주요 업무: {context.task}
- 자격 요건: {context.requirements}

[현재 분석]
{existing_block}

[다시 검토할 문항]
{question_block}
""".strip()


def _build_corpus_reference_block(context: AnalysisWorkerContextResponse) -> str:
    if not context.corpusReferences:
        return ""

    rendered_references: list[str] = []
    used_length = 0
    sorted_references = sorted(context.corpusReferences, key=lambda item: item.rank)
    for item in sorted_references[:MAX_CORPUS_REFERENCE_ITEMS]:
        separator_length = 2 if rendered_references else 0
        available_length = MAX_CORPUS_REFERENCES_LENGTH - used_length - separator_length
        prefix = (
            f"{item.category} rank={item.rank}\n"
            f"- 제목: {item.title}\n"
            "- 내용:\n"
        )
        if available_length <= len(prefix):
            break

        content = item.content[:MAX_CORPUS_REFERENCE_CONTENT_LENGTH]
        rendered = prefix + content[:available_length - len(prefix)]
        rendered_references.append(rendered)
        used_length += separator_length + len(rendered)
        if len(rendered) >= available_length:
            break

    references = "\n\n".join(rendered_references)
    return f"""
[직무 평가 기준 (Curated Corpus)]
{references}
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
[유사 채용공고 참고]
{references}
""".strip()


def _build_rag_priority_block(context: AnalysisWorkerContextResponse) -> str:
    if not context.corpusReferences and not context.similarJobPostings:
        return ""

    return """
[RAG Context 우선순위 및 사용 규칙]
- 현재 분석 대상 채용공고가 항상 최우선 평가 기준이다.
- Curated Corpus는 직무 평가 기준으로만 사용한다.
- Similar JobPosting은 실제 유사 공고의 표현과 요구 역량을 이해하기 위한 보조 참고 자료로만 사용한다.
- Curated Corpus와 Similar JobPosting이 충돌하면 Curated Corpus를 우선한다.
- 현재 채용공고와 Curated Corpus 또는 Similar JobPosting이 충돌하면 현재 채용공고를 따른다.
- Curated Corpus나 Similar JobPosting에만 있는 요구사항을 현재 공고의 필수 조건, 누락 키워드 또는 감점 근거로 사용하지 않는다.
- 현재 자기소개서 문항과 답변은 모든 참고 자료보다 우선하며, 참고 자료를 근거로 지원자의 경험, 성과, 역할 또는 계획을 추정하거나 만들어내지 않는다.
- 자기소개서 원문에 없는 사실을 improvement에 추가하지 않는다.
""".strip()
