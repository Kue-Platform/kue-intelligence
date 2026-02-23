from app.core.config import settings
from app.inngest.runtime import inngest_functions


def test_all_ingestion_functions_have_retry_limit_and_failure_handler() -> None:
    for fn in inngest_functions:
        assert fn._opts.retries == settings.inngest_max_retries
        assert fn._opts.on_failure is not None
