from pathlib import Path

from echodataflow.utils.file_watcher import FileUpdateHandler


class FakeEvent:
    is_directory = False

    def __init__(self, src_path: Path):
        self.src_path = str(src_path)


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