# Render Background Worker Deploy Checklist

## 1. Render 서비스 생성

- Render에서 `New +` -> `Blueprint` 또는 `Background Worker`를 선택한다.
- 저장소를 연결한다.
- `render.yaml`을 사용할 경우 Blueprint로 생성한다.

## 2. 배포 전 확인

- [ ] `Dockerfile`이 현재 디렉터리를 기준으로 빌드되도록 설정되어 있다.
- [ ] 워커 시작 명령이 `uvicorn app.main:app --host 0.0.0.0 --port 8000` 로 설정되어 있다.
- [ ] Render에서 RabbitMQ와 Spring API에 네트워크 접근이 가능하다.
- [ ] OpenAI API 키를 Render 환경변수에 등록할 수 있다.

## 3. 필수 환경변수

아래 값들은 `.env` 또는 Render 환경변수로 맞춰야 한다.

### 공통

- [ ] `WORKER_ENV`
- [ ] `SPRING_API_BASE_URL`
- [ ] `APP_WORKER_INTERNAL_API_KEY`
- [ ] `OPENAI_API_KEY`

### RabbitMQ 연결

- [ ] `RABBITMQ_HOST`
- [ ] `RABBITMQ_PORT`
- [ ] `RABBITMQ_USERNAME`
- [ ] `RABBITMQ_PASSWORD`
- [ ] `RABBITMQ_VHOST`
- [ ] `WORKER_PREFETCH_COUNT`
- [ ] `WORKER_DEFAULT_CONCURRENCY_LIMIT`
- [ ] `WORKER_ANALYSIS_CONCURRENCY_LIMIT`
- [ ] `WORKER_JOB_POSTING_CONCURRENCY_LIMIT`

### Job Posting Queue

- [ ] `APP_WORKER_JOB_POSTING_QUEUE`
- [ ] `APP_WORKER_JOB_POSTING_ROUTING_KEY`
- [ ] `APP_WORKER_JOB_POSTING_EXCHANGE`
- [ ] `APP_WORKER_JOB_POSTING_DLQ`

### Analysis Queue

- [ ] `APP_WORKER_ANALYSIS_QUEUE`
- [ ] `APP_WORKER_ANALYSIS_ROUTING_KEY`
- [ ] `APP_WORKER_ANALYSIS_EXCHANGE`
- [ ] `APP_WORKER_ANALYSIS_DLQ`
- [ ] `APP_WORKER_ANALYSIS_MAX_RETRY_COUNT`
- [ ] `APP_WORKER_ANALYSIS_QUEUE_TIMEOUT_MILLIS`

### OpenAI 모델 및 워커 정책

- [ ] `OPENAI_JOB_POSTING_MODEL`
- [ ] `OPENAI_ANALYSIS_MODEL`
- [ ] `JOB_POSTING_CLASSIFICATION_CONFIDENCE_THRESHOLD`
- [ ] `WORKER_MAX_RETRY_COUNT`

## 4. 배포 후 로그 확인

- [ ] `Worker process started.` 로그가 찍힌다.
- [ ] `RabbitMQ consumer started.` 로그가 찍힌다.
- [ ] 재시도 없이 큐 연결이 안정적으로 유지된다.

## 5. 운영 점검

- [ ] 테스트 메시지를 큐에 넣었을 때 Spring API까지 완료 콜백이 전달된다.
- [ ] OpenAI 호출 실패 시 재시도 정책이 의도대로 동작한다.
- [ ] DLQ 라우팅이 정상 동작한다.
- [ ] Render 재배포 후 워커가 자동으로 다시 연결된다.
- [ ] `GET /metrics` 가 Prometheus scrape 가능한 포맷으로 응답한다.
