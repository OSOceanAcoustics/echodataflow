from collections.abc import Callable
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer


class FileUpdateHandler(FileSystemEventHandler):
    """Run a callback when the target file is modified."""

    def __init__(
        self,
        target_file: str | Path,
        callback: Callable[[Path], None],
    ):
        self.target_file = Path(target_file).resolve()
        self.callback = callback

    def on_modified(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return

        event_path = Path(event.src_path).resolve()

        if event_path == self.target_file:
            self.callback(event_path)


def watch_file(
    target_file: str | Path,
    callback: Callable[[Path], None],
) -> Observer:
    """Start watching a file for modifications."""

    target_file = Path(target_file).resolve()

    observer = Observer()
    observer.schedule(
        FileUpdateHandler(
            target_file=target_file,
            callback=callback,
        ),
        str(target_file.parent),
        recursive=False,
    )
    observer.start()

    return observer