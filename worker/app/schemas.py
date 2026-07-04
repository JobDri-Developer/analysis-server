from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ApiEnvelope(BaseModel):
    isSuccess: bool
    code: str
    message: str
    result: Any | None = None
    error: Any | None = None


class JobPostingIngestTaskMessage(BaseModel):
    messageId: str
    taskType: str
    taskId: str
    userId: int
    rawText: str | None = None
    imageObjectKey: str | None = None
    retryCount: int = 0
    submittedAt: str


class JobPostingWorkerContextRequest(BaseModel):
    userId: int
    imageObjectKey: str | None = None


class JobPostingWorkerContextResponse(BaseModel):
    imageUrl: str | None = None


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


class JobPostingWorkerFailureRequest(BaseModel):
    errorMessage: str


class RetryableWorkerError(Exception):
    pass


class NonRetryableWorkerError(Exception):
    pass
