from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "jobdri-worker"
    environment: str = Field(default="local", alias="WORKER_ENV")
    worker_log_type: str = Field(default="worker", alias="APP_WORKER_LOG_TYPE")

    rabbitmq_host: str = Field(default="localhost", alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(default=5672, alias="RABBITMQ_PORT")
    rabbitmq_username: str = Field(default="guest", alias="RABBITMQ_USERNAME")
    rabbitmq_password: str = Field(default="guest", alias="RABBITMQ_PASSWORD")
    rabbitmq_vhost: str = Field(default="/", alias="RABBITMQ_VHOST")
    rabbitmq_queue: str = Field(default="jobdri.job-posting.ingest", alias="APP_WORKER_JOB_POSTING_QUEUE")
    rabbitmq_routing_key: str = Field(default="job-posting.ingest", alias="APP_WORKER_JOB_POSTING_ROUTING_KEY")
    rabbitmq_exchange: str = Field(default="jobdri.worker.exchange", alias="APP_WORKER_JOB_POSTING_EXCHANGE")
    rabbitmq_dlq: str = Field(default="jobdri.job-posting.ingest.dlq", alias="APP_WORKER_JOB_POSTING_DLQ")
    analysis_rabbitmq_queue: str = Field(default="jobdri.analysis.execute", alias="APP_WORKER_ANALYSIS_QUEUE")
    analysis_rabbitmq_routing_key: str = Field(
        default="analysis.execute",
        alias="APP_WORKER_ANALYSIS_ROUTING_KEY",
    )
    analysis_rabbitmq_exchange: str = Field(
        default="jobdri.worker.exchange",
        alias="APP_WORKER_ANALYSIS_EXCHANGE",
    )
    analysis_rabbitmq_dlq: str = Field(
        default="jobdri.analysis.execute.dlq",
        alias="APP_WORKER_ANALYSIS_DLQ",
    )
    rabbitmq_prefetch_count: int = Field(default=1, alias="WORKER_PREFETCH_COUNT")

    spring_api_base_url: str = Field(default="http://api:8080", alias="SPRING_API_BASE_URL")
    spring_internal_api_key: str = Field(alias="APP_WORKER_INTERNAL_API_KEY")

    openai_api_key: str = Field(alias="OPENAI_API_KEY")
    openai_job_posting_model: str = Field(default="gpt-4o-mini", alias="OPENAI_JOB_POSTING_MODEL")
    openai_analysis_model: str = Field(default="gpt-4.1-mini", alias="OPENAI_ANALYSIS_MODEL")
    job_posting_confidence_threshold: float = Field(
        default=0.65,
        alias="JOB_POSTING_CLASSIFICATION_CONFIDENCE_THRESHOLD",
    )
    worker_max_retry_count: int = Field(default=3, alias="WORKER_MAX_RETRY_COUNT")
    analysis_max_retry_count: int = Field(default=3, alias="APP_WORKER_ANALYSIS_MAX_RETRY_COUNT")
    analysis_queue_timeout_millis: int = Field(
        default=300000,
        alias="APP_WORKER_ANALYSIS_QUEUE_TIMEOUT_MILLIS",
    )
    worker_api_retry_max_attempts: int = Field(default=5, alias="APP_WORKER_API_RETRY_MAX_ATTEMPTS")
    worker_api_retry_base_delay_millis: int = Field(default=500, alias="APP_WORKER_API_RETRY_BASE_DELAY_MILLIS")
    worker_api_retry_max_delay_millis: int = Field(default=10000, alias="APP_WORKER_API_RETRY_MAX_DELAY_MILLIS")
    worker_recovery_spool_dir: str = Field(default=".worker-spool", alias="APP_WORKER_RECOVERY_SPOOL_DIR")
    worker_terminal_message_dir: str = Field(
        default=".worker-spool/terminal-messages",
        alias="APP_WORKER_TERMINAL_MESSAGE_DIR",
    )
    worker_recovery_poll_interval_seconds: int = Field(
        default=15,
        alias="APP_WORKER_RECOVERY_POLL_INTERVAL_SECONDS",
    )


settings = Settings()
