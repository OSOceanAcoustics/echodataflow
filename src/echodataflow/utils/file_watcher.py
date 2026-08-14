from collections.abc import Callable
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer


class FileUpdateHandler(FileSystemEventHandler):
    """Run a callback when the target file is updated."""

    def __init__(
        self,
        target_file: str | Path,
        callback: Callable[[Path], None],
    ):
        self.target_file = Path(target_file).resolve()
        self.callback = callback

    def _handle_path(self, path: str | Path) -> None:
        event_path = Path(path).resolve()

        if event_path == self.target_file:
            self.callback(event_path)

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self._handle_path(event.src_path)

    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self._handle_path(event.src_path)

    def on_moved(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self._handle_path(event.dest_path)


class FileCreatedHandler(FileSystemEventHandler):
    """Run a callback when a matching file is created or modified."""

    def __init__(
        self,
        callback: Callable[[Path], None],
        pattern: str,
    ):
        self.callback = callback
        self.pattern = pattern

    def _handle(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return

        event_path = Path(event.src_path).resolve()

        if event_path.match(self.pattern):
            self.callback(event_path)

    def on_created(self, event: FileSystemEvent) -> None:
        self._handle(event)

    def on_modified(self, event: FileSystemEvent) -> None:
        self._handle(event)


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


def watch_directory(
    directory: str | Path,
    callback: Callable[[Path], None],
    pattern: str,
) -> Observer:
    """Start watching a directory for matching file changes."""

    directory = Path(directory).resolve()

    observer = Observer()
    observer.schedule(
        FileCreatedHandler(
            callback=callback,
            pattern=pattern,
        ),
        str(directory),
        recursive=False,
    )
    observer.start()

    return observer