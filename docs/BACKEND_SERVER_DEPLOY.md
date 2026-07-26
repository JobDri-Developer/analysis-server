# Backend Server Worker Deploy

## 서버 배치 방식

- 워커는 레포의 `deploy/docker-compose.worker.prod.yml` 을 서버에 업로드한 뒤 단독으로 관리한다.
- 워커는 `8000` 포트에서 `/health`, `/metrics` 를 노출한다.

## 서버에 둘 파일

- `docker-compose.worker.prod.yml`
- `.env`

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
- `WORKER_METRICS_PORT`

## 서버에서 수동 배포할 때

```bash
docker login ghcr.io
export WORKER_IMAGE_NAME=ghcr.io/jobdri-developer/analysis-worker
export WORKER_IMAGE_TAG=latest
docker compose -f docker-compose.worker.prod.yml pull worker
docker compose -f docker-compose.worker.prod.yml up -d worker
```

## GitHub Actions secrets

- `DEPLOY_HOST`
- `DEPLOY_PORT`
- `DEPLOY_USER`
- `DEPLOY_SSH_KEY`
- `DEPLOY_PATH`
- `GHCR_USERNAME`
- `GHCR_TOKEN`

`DEPLOY_PATH` 는 백엔드 서버에서 `.env` 를 관리하는 디렉터리여야 한다. GitHub Actions가 같은 경로로 `docker-compose.worker.prod.yml` 을 업로드한다.
