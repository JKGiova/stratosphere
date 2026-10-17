import pytest

from stratosphere.research import worker


def test_worker_rejects_changed_code_and_data_before_engine_execution(
    research_plan,
    research_snapshot,
    monkeypatch,
):
    def unexpected(*args, **kwargs):
        pytest.fail("The engine must not execute unbound inputs")

    monkeypatch.setattr(worker, "run_simulation", unexpected)
    request = {
        "plan": research_plan.model_dump(mode="json"),
        "bars": [bar.model_dump(mode="json") for bar in research_snapshot.bars],
        "normalized_sha256": research_snapshot.normalized_sha256,
    }
    request["plan"]["implementation_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="Implementation"):
        worker.execute(request, 1)
    request["plan"] = research_plan.model_dump(mode="json")
    request["bars"][0]["close"] = "101"
    with pytest.raises(ValueError, match="integrity"):
        worker.execute(request, 1)
