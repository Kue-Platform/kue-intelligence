from app.tasks.data_tasks import analyze_numeric_series


def test_analyze_numeric_series() -> None:
    output = analyze_numeric_series.run([1, 2, 3, 4])

    assert output["count"] == 4.0
    assert output["mean"] == 2.5
    assert output["median"] == 2.5
    assert output["min"] == 1.0
    assert output["max"] == 4.0
    assert output["sum"] == 10.0
