import pytest

import echodataflow.flows.flows_acoustics as flows_acoustics

from echodataflow.flows.flows_acoustics import (
    flow_create_MVBS_postprocessing,
)
from echodataflow.flows.flows_predict_hake import flow_predict_hake_postprocessing


@pytest.mark.parametrize(
    ("flow_function", "parameters"),
    [
        (
            flow_predict_hake_postprocessing.fn,
            {"path_main": "output", "path_weight": "weights.ckpt"},
        ),
    ],
)
@pytest.mark.parametrize(
    ("start_time", "end_time"),
    [
        (None, None),
        ("2025-06-19T00:00:00Z", None),
        (None, "2025-06-19T01:00:00Z"),
    ],
)
def test_overwrite_requires_bounded_time_range(
    flow_function,
    parameters,
    start_time,
    end_time,
):
    with pytest.raises(
        ValueError,
        match="overwrite=True requires explicit start_time and end_time",
    ):
        flow_function(
            **parameters,
            overwrite=True,
            start_time=start_time,
            end_time=end_time,
        )


def test_mvbs_flow_skips_when_sv_ledger_does_not_exist(tmp_path, monkeypatch):
    messages = []

    class Logger:
        def info(self, message):
            messages.append(message)

    monkeypatch.setattr(flows_acoustics, "get_run_logger", lambda: Logger())

    flow_create_MVBS_postprocessing.fn(path_main=str(tmp_path))

    assert messages == ["Sv ledger does not yet exist"]
    assert not (tmp_path / "MVBS_files.csv").exists()
