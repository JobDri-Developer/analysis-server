## Worker Internal API Contract

기준일: 2026-07-24

현재 worker는 Spring internal API contract를 아래처럼 해석한다.

- repeated callback 또는 같은 payload 재전송은 `409 conflict` 가 아니라 `200` 계열 `isSuccess=true` no-op 성공으로 처리된다고 본다.
- worker는 더 이상 `409 conflict => 멱등 성공` 에 의존하지 않는다.
- `200` 계열 응답에서 `code` 가 `ALREADY_COMPLETED`, `NO_OP` 같은 no-op 의미여도, `isSuccess=true` 이면 성공으로 처리한다.
- `409` 는 더 이상 멱등 성공 신호가 아니며, 일반적인 4xx contract mismatch로 간주한다.

### Job Posting Canonical Flow

기본 흐름은 아래 순서를 따른다.

1. `context`
2. `candidates` (필요 시)
3. `result` (`JobPostingWorkerFinalizeRequest` payload 저장)
4. `finalize`

`complete` endpoint는 legacy compatibility 용도로만 남아 있으며, 새 기본 경로에서는 사용하지 않는다.

저신뢰도 분기 역시 기본적으로 `result -> finalize` 를 사용한다. 이때 worker는 canonical finalize payload를 만들기 위해 추출 결과 기반 `generated` 값을 채운다.

### Analysis Flow

analysis 는 기존처럼 아래 순서를 유지한다.

1. `context`
2. `result`
3. `complete`

analysis 의 repeated `result`/`complete` callback 역시 서버가 `200` no-op 성공을 반환하면 worker는 성공으로 처리한다.
