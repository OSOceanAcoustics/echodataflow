from pathlib import Path

from echodataflow.utils import raw_monitor


def test_emit_raw_update_event(monkeypatch, tmp_path):
    raw_file = tmp_path / "example.raw"
    raw_file.touch()

    emitted = {}

    def fake_emit_event(**kwargs):
        emitted.update(kwargs)

    monkeypatch.setattr(
        raw_monitor,
        "emit_event",
        fake_emit_event,
    )

    raw_monitor.emit_raw_update_event(raw_file)

    assert emitted["event"] == "echodataflow.raw.updated"
    assert emitted["resource"]["prefect.resource.id"] == "raw-monitor"
    assert emitted["resource"]["prefect.resource.name"] == "raw-monitor"
    assert emitted["resource"]["path"] == str(raw_file)


def test_watch_raw_directory(monkeypatch, tmp_path):
    called = {}

    def fake_watch_directory(
        directory,
        callback,
        pattern,
    ):
        called["directory"] = directory
        called["callback"] = callback
        called["pattern"] = pattern
        return "observer"

    monkeypatch.setattr(
        raw_monitor,
        "watch_directory",
        fake_watch_directory,
    )

    result = raw_monitor.watch_raw_directory(tmp_path)

    assert result == "observer"
    assert called["directory"] == tmp_path
    assert called["callback"] is raw_monitor.emit_raw_update_event
    assert called["pattern"] == "*.raw"