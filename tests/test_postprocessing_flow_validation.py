import pandas as pd

import echodataflow.flows.flows_acoustics as flows_acoustics
import echodataflow.flows.flows_predict_hake as flows_predict_hake

from echodataflow.flows.flows_acoustics import (
    flow_create_MVBS_postprocessing,
    flow_raw2Sv_postprocessing,
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


def test_raw2sv_postprocessing_skips_excluded_raw_files(tmp_path, monkeypatch, capsys):
    raw_filename = "IWCPS-D20250619-T000204.raw"
    ledger = pd.DataFrame(
        {
            "s3_path": [f"survey/{raw_filename}"],
            "timestamp": [pd.Timestamp("2025-06-19T00:02:04Z")],
            "raw_filename": [raw_filename],
            "Sv_filename": [pd.NA],
            "raw2Sv_status": ["pending"],
            "first_ping_time": [pd.NaT],
            "last_ping_time": [pd.NaT],
            "error": [""],
        }
    )
    messages = []

    class Logger:
        def info(self, message):
            messages.append(message)

    monkeypatch.setattr(flows_acoustics, "get_run_logger", lambda: Logger())
    monkeypatch.setattr(
        flows_acoustics,
        "read_or_create_ledger",
        lambda **_: ledger.copy(),
    )

    flow_raw2Sv_postprocessing.fn(
        path_raw_list="unused.csv",
        path_raw=str(tmp_path),
        path_main=str(tmp_path),
        exclude_raw_file=[raw_filename],
    )

    assert messages == ["No raw files require processing"]
    assert f"Exclude ['{raw_filename}'] from processing" in capsys.readouterr().out


def test_mvbs_postprocessing_applies_new_file_num_limit(tmp_path, monkeypatch):
    (tmp_path / "Sv_files.csv").touch()
    messages = []

    class Logger:
        def info(self, message):
            messages.append(message)

    monkeypatch.setattr(flows_acoustics, "get_run_logger", lambda: Logger())
    monkeypatch.setattr(flows_acoustics, "read_manifest", lambda **_: pd.DataFrame())
    monkeypatch.setattr(
        flows_acoustics,
        "read_or_create_ledger",
        lambda **_: pd.DataFrame(),
    )
    monkeypatch.setattr(flows_acoustics, "plan_mvbs_slices", lambda *_: [object()])

    flow_create_MVBS_postprocessing.fn(
        path_main=str(tmp_path),
        new_file_num_limit=0,
    )

    assert messages == ["No newly ready MVBS slices"]
