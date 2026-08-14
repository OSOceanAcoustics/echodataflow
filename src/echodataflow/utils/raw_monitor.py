from pathlib import Path

from prefect.events import emit_event

from echodataflow.utils.file_watcher import watch_directory


RAW_UPDATE_EVENT = "echodataflow.raw.updated"
RAW_RESOURCE_ID = "raw-monitor"


def emit_raw_update_event(path: Path) -> None:
    """Emit a Prefect event when a RAW file arrives."""

    emit_event(
        event=RAW_UPDATE_EVENT,
        resource={
            "prefect.resource.id": RAW_RESOURCE_ID,
            "prefect.resource.name": RAW_RESOURCE_ID,
            "path": str(path),
        },
    )


def watch_raw_directory(path: str | Path):
    """Watch a directory for new RAW files."""

    return watch_directory(
        directory=path,
        callback=emit_raw_update_event,
        pattern="*.raw",
    )