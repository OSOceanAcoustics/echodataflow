import echodataflow.flows.flows_acoustics as flows_acoustics
import echodataflow.flows.flows_predict_hake as flows_predict_hake

from echodataflow.flows.flows_acoustics import (
    flow_create_MVBS_postprocessing,
)
from echodataflow.flows.flows_predict_hake import flow_predict_hake_postprocessing


def test_mvbs_flow_skips_when_sv_ledger_does_not_exist(tmp_path, monkeypatch):
    messages = []

    class Logger:
        def info(self, message):
            messages.append(message)

    monkeypatch.setattr(flows_acoustics, "get_run_logger", lambda: Logger())
    flow_create_MVBS_postprocessing.fn(path_main=str(tmp_path))

    assert messages == ["Sv ledger does not yet exist"]
    assert not (tmp_path / "MVBS_files.csv").exists()


def test_prediction_flow_skips_when_mvbs_ledger_does_not_exist(tmp_path, monkeypatch):
    messages = []

    class Logger:
        def info(self, message):
            messages.append(message)

    monkeypatch.setattr(flows_predict_hake, "get_run_logger", lambda: Logger())
    flow_predict_hake_postprocessing.fn(
        path_main=str(tmp_path),
        path_weight="unused.ckpt",
    )

    assert messages == ["MVBS ledger does not yet exist"]
    assert not (tmp_path / "prediction_files.csv").exists()
