import numpy as np
import pandas as pd

from app.celery_app import celery_app


@celery_app.task(name="app.tasks.data_tasks.analyze_numeric_series")
def analyze_numeric_series(values: list[float]) -> dict[str, float]:
    arr = np.array(values, dtype=float)
    series = pd.Series(arr)

    return {
        "count": float(series.count()),
        "mean": float(series.mean()),
        "median": float(series.median()),
        "std": float(series.std(ddof=0)),
        "min": float(series.min()),
        "max": float(series.max()),
        "sum": float(series.sum()),
    }
