from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException

from app.celery_app import celery_app
from app.schemas import AnalyzeDataRequest, JobCreatedResponse, JobStatusResponse
from app.tasks.data_tasks import analyze_numeric_series

router = APIRouter(prefix="/v1", tags=["jobs"])


@router.post("/jobs/analyze", response_model=JobCreatedResponse)
def create_analysis_job(payload: AnalyzeDataRequest) -> JobCreatedResponse:
    task = analyze_numeric_series.delay(payload.values)
    return JobCreatedResponse(task_id=task.id, status=task.status)


@router.get("/jobs/{task_id}", response_model=JobStatusResponse)
def get_job_status(task_id: str) -> JobStatusResponse:
    result = AsyncResult(task_id, app=celery_app)

    if result.failed():
        return JobStatusResponse(
            task_id=task_id,
            status=result.status,
            error=str(result.result),
        )

    if result.successful():
        return JobStatusResponse(
            task_id=task_id,
            status=result.status,
            result=result.result,
        )

    if result.status in {"PENDING", "STARTED", "RETRY", "RECEIVED"}:
        return JobStatusResponse(task_id=task_id, status=result.status)

    raise HTTPException(status_code=500, detail=f"Unknown task status: {result.status}")
