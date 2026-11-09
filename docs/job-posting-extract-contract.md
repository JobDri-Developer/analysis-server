# Java·Python 공고 추출 응답 계약 (#396 일부)

`JobPostingExtractResponse`의 필드명은 두 저장소에서 동일한 camelCase를 사용한다.
이번 변경은 기존 추출 모델에 requiredSkills·deadlineAt을 맞추는 범위다. 분석 후 폼 확인·등록 기능 전체를 완료한 것은 아니다.

| 필드 | Java / Python 타입 | 미입력 값 |
| --- | --- | --- |
| requiredSkills | List<String> / list[str] | [] |
| deadlineAt | String / str | 빈 문자열 |

새 필드가 없는 구버전 응답과 두 필드에 명시적 null이 들어온 응답 모두 []·빈 문자열로 정규화한다.
기존 postingName·companyName·jobTitle·task·requirements·preferredQualifications·rawText·confidence 계약은 유지한다.

## 추출 규칙

- requiredSkills는 원문에 명시된 기술명만 중복 없이 추출한다. 없으면 빈 배열이다.
- deadlineAt은 원문에서 확인 가능한 마감 시각을 yyyy-MM-dd'T'HH:mm:ss 형식으로 반환한다. 확인할 수 없으면 빈 문자열이다.
- 시간대 오프셋/Z를 붙이지 않는 기존 Java local date-time 계약을 따른다. 확인하지 못한 날짜·시각을 임의 생성하지 않는다.
- DTO에서는 날짜를 String으로 유지한다. 모델에 필드가 추가됐다고 날짜 형식 검증이나 LLM 추출 정확도를 보장하지 않는다.
- Python의 동기·비동기 추출은 공통 프롬프트·파서를 사용한다. 분류 요청·finalize·저장 결과의 nested extracted에도 필드를 유지한다.

## 검증과 배포 순서

두 저장소에 같은 job-posting-worker-finalize.json 샘플을 둬 독립적으로 계약을 검증한다.
BackEnd: src/test/resources/contracts/job-posting-worker-finalize.json
analysis-server: tests/fixtures/job-posting-worker-finalize.json

Python에서는 파싱·동기/비동기 mock 추출·저장 결과 JSON 재직렬화를 확인하고 Java에서는 같은 샘플의 역직렬화 및 저장 envelope 재직렬화를 확인한다.
구버전 Python은 새 필드를 버릴 수 있으므로 필드 활용 전 worker 모델·프롬프트 변경을 배포해야 한다. Java에는 이미 필드가 있으며 이번에는 null/생략 기본값을 명확히 한다.
라이브 OpenAI·RabbitMQ·DB·컨테이너 연동이나 실공고 정확도 평가는 별도다.

## 리뷰 보완: 배포·롤백 제약

- 배포 순서: 두 필드를 처리하는 BackEnd를 먼저 배포한다. 그 뒤 worker 이미지를 배포한다. 워커 workflow는 latest 대신 해당 github.sha 태그를 사용한다.
- workflow는 새 이미지로 `check_backend_contract()`를 먼저 실행하고 성공한 경우에만 worker를 교체한다. 데이터 쓰기 없이 실제 SPRING_API_BASE_URL의 `/v3/api-docs`에서 JobPostingExtractResponse의 requiredSkills(array of string)·deadlineAt(string)을 검사한다.
- WORKER_ENV=prod에서는 queue 소비 시작 전과 각 JOB_POSTING_INGEST HTTP 요청 직전에 동일 계약을 다시 검사한다. 비호환·조회 실패는 fail closed다. 비호환 백엔드 롤백 후에도 캐시된 이전 검사로 데이터를 전송하지 않는다.
- 운영에서 워커가 `/v3/api-docs`에 접근할 수 있어야 한다. 문서 endpoint를 비활성화/차단하면 배포와 공고 처리가 차단된다. 스키마만으로 실제 저장 구현을 증명할 수는 없으므로 배포 전 저장·조회 smoke test가 필요하다.
- 롤백: 두 필드를 처리하는 호환 BackEnd 버전으로만 롤백한다. 비호환 버전이 꼭 필요하면 먼저 워커 소비를 멈추고 호환 worker/BackEnd 조합을 함께 적용한다. 미전달 결과·recovery spool과 기존 저장 payload를 보존한다.
- 이 저장소는 BackEnd 배포 workflow나 다른 운영자의 직접 롤백을 제어하지 못한다. 검사와 실제 요청 사이 버전 교체 및 여러 인스턴스 혼재를 원자적으로 막지 못한다. 운영에서는 호환 버전만 남기는 배포·롤백 절차가 추가로 필요하다. 이번 작업에서 서버의 실제 설정·릴리스 순서나 운영 rollback을 실행 검증하지 않았다.
- 요청마다 스키마를 확인하므로 추가 HTTP 지연과 문서 endpoint 가용성 의존이 생긴다. 추후 전용 capability/version handshake로 대체할 수 있다.

## 리뷰 보완: 태그 및 검증 범위

requiredSkills는 Pydantic의 list[str] 검증 이후 정확히 같은 문자열의 중복만 제거한다. 첫 등장 순서를 유지하며 대소문자 변경·공백 정규화는 하지 않는다.
원래 모델 직렬화 테스트에 더해 동기·비동기 추출의 실제 input_payload 프롬프트와 API client의 HTTP POST JSON, 모의 백엔드 저장 후 GET/resultPayload 파싱을 검사한다.
저장·읽기 테스트는 HTTP 경계의 in-memory mock이며 실제 BackEnd DB 영속성이나 라이브 서비스 통합 테스트는 아니다.
