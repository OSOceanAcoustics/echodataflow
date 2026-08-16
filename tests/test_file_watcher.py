from pathlib import Path

from echodataflow.utils.file_watcher import (
    FileCreatedHandler,
    FileUpdateHandler,
)

class FakeEvent:
    is_directory = False

    def __init__(
        self,
        src_path: Path,
        dest_path: Path | None = None,
    ):
        self.src_path = str(src_path)
        self.dest_path = str(dest_path) if dest_path else str(src_path)


def test_file_update_handler_calls_callback_for_target(tmp_path):
    target = tmp_path / "transect_start_end_time.csv"
    target.touch()

    detected = []

    handler = FileUpdateHandler(
        target_file=target,
        callback=detected.append,
    )

    handler.on_modified(FakeEvent(target))

    assert detected == [target.resolve()]


def test_file_update_handler_ignores_other_files(tmp_path):
    target = tmp_path / "transect_start_end_time.csv"
    other = tmp_path / "other.csv"

    target.touch()
    other.touch()

    detected = []

    handler = FileUpdateHandler(
        target_file=target,
        callback=detected.append,
    )

    handler.on_modified(FakeEvent(other))

    assert detected == []
    
    
def test_file_update_handler_calls_callback_for_created_target(tmp_path):
    target = tmp_path / "transect_start_end_time.csv"
    detected = []

    handler = FileUpdateHandler(
        target_file=target,
        callback=detected.append,
    )

    handler.on_created(FakeEvent(target))

    assert detected == [target.resolve()]


def test_file_update_handler_calls_callback_for_moved_target(tmp_path):
    target = tmp_path / "transect_start_end_time.csv"
    temporary = tmp_path / "temporary.csv"
    detected = []

    handler = FileUpdateHandler(
        target_file=target,
        callback=detected.append,
    )

    handler.on_moved(
        FakeEvent(
            src_path=temporary,
            dest_path=target,
        )
    )

    assert detected == [target.resolve()]


def test_file_created_handler_calls_callback_for_matching_file(tmp_path):
    raw_file = tmp_path / "example.raw"
    detected = []

    handler = FileCreatedHandler(
        callback=detected.append,
        pattern="*.raw",
    )

    handler.on_created(FakeEvent(raw_file))

    assert detected == [raw_file.resolve()]


def test_file_created_handler_ignores_nonmatching_file(tmp_path):
    other_file = tmp_path / "example.txt"
    detected = []

    handler = FileCreatedHandler(
        callback=detected.append,
        pattern="*.raw",
    )

    handler.on_created(FakeEvent(other_file))

    assert detected == []

def test_file_created_handler_calls_callback_for_moved_matching_file(tmp_path):
    temporary = tmp_path / "temporary.tmp"
    raw_file = tmp_path / "example.raw"
    detected = []

    handler = FileCreatedHandler(
        callback=detected.append,
        pattern="*.raw",
    )

    handler.on_moved(
        FakeEvent(
            src_path=temporary,
            dest_path=raw_file,
        )
    )

    assert detected == [raw_file.resolve()]