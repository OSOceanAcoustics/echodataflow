from pathlib import Path
from echodataflow.utils import transect_monitor


def test_emit_transect_update_event(monkeypatch, tmp_path):
    target = tmp_path / "transect_start_end_time.csv"
    target.touch()

    emitted = {}

    def fake_emit_event(**kwargs):
        emitted.update(kwargs)

    monkeypatch.setattr(
        transect_monitor,
        "emit_event",
        fake_emit_event,
    )

    transect_monitor.emit_transect_update_event(target)

    assert emitted["event"] == "echodataflow.transect.updated"
    assert emitted["resource"]["prefect.resource.id"] == "transect-start-end-time"
    assert emitted["resource"]["path"] == str(target)
    