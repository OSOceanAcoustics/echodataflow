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
    db_path = tmp_path / "processing.db"

    def fake_initialize_ledger(path):
        called["db_path"] = path

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
        "initialize_ledger",
        fake_initialize_ledger,
    )

    monkeypatch.setattr(
        raw_monitor,
        "watch_directory",
        fake_watch_directory,
    )

    result = raw_monitor.watch_raw_directory(
        tmp_path,
        db_path,
    )

    assert result == "observer"
    assert called["directory"] == tmp_path
    assert called["db_path"] == db_path
    assert called["pattern"] == "*.raw"


def test_register_and_emit_raw_update(monkeypatch, tmp_path):
    raw_file = tmp_path / "example.raw"
    db_path = tmp_path / "processing.db"

    called = {}

    def fake_register_raw_file(db, path):
        called["registered"] = (db, path)

    def fake_emit_raw_update_event(path):
        called["emitted"] = path

    monkeypatch.setattr(
        raw_monitor,
        "register_raw_file",
        fake_register_raw_file,
    )

    monkeypatch.setattr(
        raw_monitor,
        "emit_raw_update_event",
        fake_emit_raw_update_event,
    )

    raw_monitor.register_and_emit_raw_update(
        raw_file,
        db_path,
    )

    assert called["registered"] == (db_path, raw_file)
    assert called["emitted"] == raw_file
