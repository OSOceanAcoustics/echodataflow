from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from echodataflow.operations import ops_acoustics


class FakeSvDataset:
    def __init__(self):
        self.selection = None

    def sel(self, **kwargs):
        self.selection = kwargs
        return self


class FakeMVBSDataset:
    def __init__(self):
        self.chunks = None
        self.saved = None

    def __getitem__(self, key):
        assert key == "ping_time"
        return [
            SimpleNamespace(values="2026-01-01T00:00:05"),
            SimpleNamespace(values="2026-01-01T00:09:55"),
        ]

    def chunk(self, chunks):
        self.chunks = chunks
        return self

    def to_zarr(self, **kwargs):
        self.saved = kwargs


def test_create_MVBS_processes_one_time_slice(monkeypatch, tmp_path):
    sv_dataset = FakeSvDataset()
    mvbs_dataset = FakeMVBSDataset()
    open_call = {}
    compute_call = {}

    def fake_open_mfdataset(paths, **kwargs):
        open_call.update(paths=paths, kwargs=kwargs)
        return sv_dataset

    def fake_compute_MVBS(**kwargs):
        compute_call.update(kwargs)
        return mvbs_dataset

    monkeypatch.setattr(ops_acoustics.xr, "open_mfdataset", fake_open_mfdataset)
    monkeypatch.setattr(
        ops_acoustics.ep.commongrid,
        "compute_MVBS",
        fake_compute_MVBS,
    )

    item = ops_acoustics.CreateMVBSWorkItem(
        start_time=pd.Timestamp("2026-01-01T00:00:00Z"),
        end_time=pd.Timestamp("2026-01-01T00:10:00Z"),
        sv_filenames=("first_Sv.zarr", "second_Sv.zarr"),
        mvbs_filename="MVBS_20260101T000000.zarr",
    )
    settings = ops_acoustics.CreateMVBSSettings(
        sv_directory=str(tmp_path / "Sv"),
        output_directory=str(tmp_path / "MVBS"),
        range_bin="1m",
        ping_time_bin="5s",
    )

    result = ops_acoustics.create_MVBS(item, settings)

    assert open_call == {
        "paths": [
            tmp_path / "Sv" / "first_Sv.zarr",
            tmp_path / "Sv" / "second_Sv.zarr",
        ],
        "kwargs": {
            "parallel": True,
            "coords": "minimal",
            "data_vars": "minimal",
            "compat": "override",
            "chunks": {"channel": 1, "ping_time": 1000, "range_sample": -1},
            "engine": "zarr",
        },
    }
    assert sv_dataset.selection == {
        "ping_time": slice(
            pd.Timestamp("2026-01-01T00:00:00"),
            pd.Timestamp("2026-01-01T00:10:00")
            - pd.to_timedelta("1nanoseconds"),
        )
    }
    assert compute_call["ds_Sv"] is sv_dataset
    assert compute_call["range_bin"] == "1m"
    assert compute_call["ping_time_bin"] == "5s"
    assert mvbs_dataset.chunks == {"channel": -1, "ping_time": -1, "depth": -1}
    assert mvbs_dataset.saved == {
        "store": tmp_path / "MVBS" / "MVBS_20260101T000000.zarr",
        "mode": "w",
        "consolidated": True,
    }
    assert result == ops_acoustics.CreateMVBSResult(
        mvbs_filename="MVBS_20260101T000000.zarr",
        first_ping_time=pd.Timestamp("2026-01-01T00:00:05"),
        last_ping_time=pd.Timestamp("2026-01-01T00:09:55"),
    )
