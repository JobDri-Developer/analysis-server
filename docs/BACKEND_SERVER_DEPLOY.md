# Backend Server Worker Deploy

## 서버 배치 방식

- 백엔드 서버의 기존 `docker-compose.prod.yml` 은 그대로 유지한다.
- 워커는 `docker-compose.worker.prod.yml` 파일을 추가로 올려 compose override 형태로 함께 띄운다.
- 워커는 외부 포트를 열지 않고 Spring API와 RabbitMQ에만 연결한다.

## 서버에 둘 파일

### 1. 기존 백엔드 파일

- `docker-compose.prod.yml`
- `.env`

### 2. 새로 추가할 워커 파일

- `docker-compose.worker.prod.yml`

`docker-compose.worker.prod.yml` 내용은 [`deploy/docker-compose.worker.prod.yml.example`](/Users/shinae/Desktop/study/analysis-server/worker/deploy/docker-compose.worker.prod.yml.example:1) 를 기준으로 서버에 복사하면 된다.

## 서버 `.env` 에 필요한 값

- `APP_WORKER_INTERNAL_API_KEY`
- `RABBITMQ_HOST`
- `RABBITMQ_PORT`
- `RABBITMQ_USERNAME`
- `RABBITMQ_PASSWORD`
- `RABBITMQ_VHOST`
- `APP_WORKER_JOB_POSTING_EXCHANGE`
- `APP_WORKER_JOB_POSTING_QUEUE`
- `APP_WORKER_JOB_POSTING_ROUTING_KEY`
- `APP_WORKER_JOB_POSTING_DLQ`
- `APP_WORKER_ANALYSIS_EXCHANGE`
- `APP_WORKER_ANALYSIS_QUEUE`
- `APP_WORKER_ANALYSIS_ROUTING_KEY`
- `APP_WORKER_ANALYSIS_DLQ`
- `WORKER_SPRING_API_BASE_URL`
- `OPENAI_API_KEY`
- `OPENAI_JOB_POSTING_MODEL`
- `OPENAI_ANALYSIS_MODEL`
- `JOB_POSTING_CLASSIFICATION_CONFIDENCE_THRESHOLD`
- `WORKER_MAX_RETRY_COUNT`
- `APP_WORKER_ANALYSIS_MAX_RETRY_COUNT`
- `APP_WORKER_ANALYSIS_QUEUE_TIMEOUT_MILLIS`

## 서버에서 수동 배포할 때

```bash
docker login ghcr.io
export WORKER_IMAGE_NAME=ghcr.io/jobdri-developer/analysis-worker
export WORKER_IMAGE_TAG=latest
docker compose -f docker-compose.prod.yml -f docker-compose.worker.prod.yml pull worker
docker compose -f docker-compose.prod.yml -f docker-compose.worker.prod.yml up -d worker
```

## GitHub Actions secrets

- `DEPLOY_HOST`
- `DEPLOY_PORT`
- `DEPLOY_USER`
- `DEPLOY_SSH_KEY`
- `DEPLOY_PATH`
- `GHCR_USERNAME`
- `GHCR_TOKEN`

`DEPLOY_PATH` 는 백엔드 서버에서 `docker-compose.prod.yml`, `.env`, `docker-compose.worker.prod.yml` 이 있는 디렉터리여야 한다.
