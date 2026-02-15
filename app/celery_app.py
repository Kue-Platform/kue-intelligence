from celery import Celery

from app.core.config import settings


celery_app = Celery(
    "kue_intelligence",
    broker=settings.redis_broker_url,
    backend=settings.redis_result_backend,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
)

celery_app.autodiscover_tasks(["app.tasks"])
