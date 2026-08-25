import pytest
from types import SimpleNamespace

pytestmark = pytest.mark.skip(
    reason="Temporarily disabled while flows_acoustics.py is reverted to 3c1b9d3"
)

from echodataflow.flows import flows_acoustics


def test_flow_raw2sv_uses_processing_ledger(monkeypatch, tmp_path):
    raw_file = tmp_path / "example.raw"

    calls = {
        "processing": [],
        "completed": [],
    }

    monkeypatch.setattr(
        flows_acoustics,
        "initialize_ledger",
        lambda db_path: None,
    )

    monkeypatch.setattr(
        flows_acoustics,
        "get_raw_files_to_process",
        lambda db_path, limit: [raw_file],
    )

    monkeypatch.setattr(
        flows_acoustics,
        "mark_raw_processing",
        lambda db_path, path: calls["processing"].append(path),
    )

    monkeypatch.setattr(
        flows_acoustics,
        "mark_raw_completed",
        lambda db_path, path, sv_filename, first_ping_time, last_ping_time:
            calls["completed"].append(
                (
                    path,
                    sv_filename,
                    first_ping_time,
                    last_ping_time,
                )
            ),
    )

    class FakeTask:
        def with_options(self, **kwargs):
            return self

        def __call__(self, work_item, settings):
            return SimpleNamespace(
                filename_Sv="example_Sv.zarr",
                first_ping_time="2026-08-14T12:00:00Z",
                last_ping_time="2026-08-14T12:05:00Z",
            )

    monkeypatch.setattr(
        flows_acoustics,
        "task_raw2Sv",
        FakeTask(),
    )

    flows_acoustics.flow_raw2Sv.fn(
        path_main=str(tmp_path),
        new_file_num_limit=1,
    )

    assert calls["processing"] == [raw_file]
    assert calls["completed"] == [
        (
            raw_file,
            "example_Sv.zarr",
            "2026-08-14T12:00:00Z",
            "2026-08-14T12:05:00Z",
        )
    ]


def test_flow_raw2sv_marks_failed(monkeypatch, tmp_path):
    raw_file = tmp_path / "example.raw"

    calls = {
        "failed": [],
    }

    monkeypatch.setattr(
        flows_acoustics,
        "initialize_ledger",
        lambda db_path: None,
    )

    monkeypatch.setattr(
        flows_acoustics,
        "get_raw_files_to_process",
        lambda db_path, limit: [raw_file],
    )

    monkeypatch.setattr(
        flows_acoustics,
        "mark_raw_processing",
        lambda db_path, path: None,
    )

    monkeypatch.setattr(
        flows_acoustics,
        "mark_raw_failed",
        lambda db_path, path, error: calls["failed"].append((path, error)),
    )

    class FakeTask:
        def with_options(self, **kwargs):
            return self

        def __call__(self, work_item, settings):
            raise RuntimeError("conversion failed")

    monkeypatch.setattr(
        flows_acoustics,
        "task_raw2Sv",
        FakeTask(),
    )

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def set_flow_run_state(self, **kwargs):
            pass

    monkeypatch.setattr(
        flows_acoustics,
        "get_client",
        lambda: FakeClient(),
    )

    monkeypatch.setattr(
        flows_acoustics.runtime.flow_run,
        "id",
        "00000000-0000-0000-0000-000000000001",
    )

    with pytest.raises(RuntimeError, match="1 errors during raw to Sv conversion"):
        flows_acoustics.flow_raw2Sv.fn(
            path_main=str(tmp_path),
            new_file_num_limit=1,
        )

    assert calls["failed"] == [
        (raw_file, "conversion failed"),
    ]
