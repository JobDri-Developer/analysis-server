from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ApiEnvelope(BaseModel):
    isSuccess: bool
    code: str
    message: str
    result: Any | None = None
    error: Any | None = None


class JobPostingIngestTaskMessage(BaseModel):
    messageId: str
    taskType: Literal["JOB_POSTING_INGEST"]
    taskId: str
    userId: int
    rawText: str | None = None
    imageObjectKey: str | None = None
    retryCount: int = 0
    maxRetryCount: int = 0
    submittedAt: str


class JobPostingWorkerContextRequest(BaseModel):
    userId: int
    imageObjectKey: str | None = None


class JobPostingWorkerContextResponse(BaseModel):
    imageUrl: str | None = None


class JobPostingWorkerRunningRequest(BaseModel):
    workerId: str
    retryCount: int
    submittedAt: str | None = None


class JobPostingExtractResponse(BaseModel):
    companyName: str = ""
    jobTitle: str = ""
    task: str = ""
    requirements: str = ""
    preferredQualifications: str = ""
    rawText: str = ""
    confidence: float = 0.0


class JobPostingClassificationCandidateResponse(BaseModel):
    detailClassificationId: int
    detailClassificationName: str
    middleClassificationName: str
    bigClassificationName: str
    score: float


class JobPostingClassificationResultResponse(BaseModel):
    detailClassificationId: int
    detailClassificationName: str
    middleClassificationName: str
    bigClassificationName: str
    reason: str = ""
    confidence: float = 0.0


class JobPostingGenerateResponse(BaseModel):
    companyName: str = ""
    jobTitle: str = ""
    task: str = ""
    requirements: str = ""
    preferredQualifications: str = ""
    summary: str = ""


class JobPostingResponse(BaseModel):
    jobPostingId: int | None = None
    companyName: str | None = None
    companyId: int | None = None
    detailClassificationId: int | None = None
    detailClassificationName: str | None = None
    task: str | None = None
    requirement: str | None = None
    preferred: str | None = None


class JobPostingIngestResponse(BaseModel):
    savedToDatabase: bool
    message: str
    extracted: JobPostingExtractResponse
    candidates: list[JobPostingClassificationCandidateResponse]
    classification: JobPostingClassificationResultResponse
    generated: JobPostingGenerateResponse | None = None
    saved: JobPostingResponse | None = None


class JobPostingWorkerFinalizeRequest(BaseModel):
    taskId: str
    userId: int
    extracted: JobPostingExtractResponse
    candidates: list[JobPostingClassificationCandidateResponse]
    classification: JobPostingClassificationResultResponse
    generated: JobPostingGenerateResponse


FailureReason = Literal["RATE_LIMIT", "QUEUE_TIMEOUT", "OPENAI_TIMEOUT", "VALIDATION_ERROR", "INTERNAL_ERROR"]


class JobPostingWorkerRetryRequest(BaseModel):
    errorMessage: str
    failureReason: FailureReason
    retryCount: int
    workerId: str
    queueLatencyMillis: int | None = None


class JobPostingWorkerFailureRequest(BaseModel):
    errorMessage: str
    failureReason: FailureReason
    retryCount: int
    workerId: str
    queueLatencyMillis: int | None = None


class JobPostingTaskStatusResponse(BaseModel):
    status: str | None = None
    failureReason: FailureReason | None = None
    workerId: str | None = None
    retryCount: int | None = None
    maxRetryCount: int | None = None
    queueLatencyMillis: int | None = None


class AnalysisTaskMessage(BaseModel):
    messageId: str
    taskType: Literal["ANALYSIS"]
    taskId: str
    userId: int
    mockApplyId: int
    retryCount: int = 0
    maxRetryCount: int = 0
    submittedAt: str


class AnalysisWorkerContextRequest(BaseModel):
    taskId: str
    userId: int
    mockApplyId: int


class AnalysisQuestionContextResponse(BaseModel):
    questionId: int
    question: str
    answer: str
    charLimit: int | None = None


class AnalysisWorkerContextResponse(BaseModel):
    userId: int
    mockApplyId: int
    companyName: str
    jobTitle: str
    task: str
    requirements: str
    preferredQualifications: str
    bigClassificationName: str
    middleClassificationName: str
    detailClassificationName: str
    questions: list[AnalysisQuestionContextResponse] = Field(default_factory=list)


class AnalysisWorkerRunningRequest(BaseModel):
    workerId: str
    retryCount: int
    submittedAt: str | None = None


class AnalysisQuestionAnalysisResponse(BaseModel):
    questionId: int
    sentence: str
    status: str
    reason: str
    improvement: str


class AnalysisLlmResponse(BaseModel):
    jobFit: int
    impact: int
    completeness: int
    feedback: str
    questionAnalyses: list[AnalysisQuestionAnalysisResponse]


class AnalysisWorkerRetryRequest(BaseModel):
    errorMessage: str
    failureReason: FailureReason
    retryCount: int
    workerId: str
    queueLatencyMillis: int | None = None


class AnalysisWorkerFailureRequest(BaseModel):
    errorMessage: str
    failureReason: FailureReason
    retryCount: int
    workerId: str
    queueLatencyMillis: int | None = None


class AnalysisWorkerCompleteRequest(BaseModel):
    userId: int
    mockApplyId: int
    workerId: str
    queueLatencyMillis: int | None = None
    llmResponse: AnalysisLlmResponse


class AnalysisTaskStatusResponse(BaseModel):
    status: str | None = None
    failureReason: FailureReason | None = None
    workerId: str | None = None
    retryCount: int | None = None
    maxRetryCount: int | None = None
    queueLatencyMillis: int | None = None


class RetryableWorkerError(Exception):
    def __init__(
        self,
        message: str,
        *,
        failure_reason: FailureReason = "INTERNAL_ERROR",
        openai_request_id: str | None = None,
        queue_latency_millis: int | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_reason = failure_reason
        self.openai_request_id = openai_request_id
        self.queue_latency_millis = queue_latency_millis


class NonRetryableWorkerError(Exception):
    def __init__(
        self,
        message: str,
        *,
        failure_reason: FailureReason = "INTERNAL_ERROR",
        openai_request_id: str | None = None,
        queue_latency_millis: int | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_reason = failure_reason
        self.openai_request_id = openai_request_id
        self.queue_latency_millis = queue_latency_millis
