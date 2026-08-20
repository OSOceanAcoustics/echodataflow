from pathlib import Path

from prefect.events import emit_event

from echodataflow.utils.file_watcher import watch_directory
from echodataflow.utils.processing_ledger import (
    initialize_ledger,
    register_raw_file,
)


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


def register_and_emit_raw_update(path: Path, db_path: str | Path) -> None:
    """Register a RAW file in the ledger, then emit its Prefect event."""
    register_raw_file(db_path, path)
    emit_raw_update_event(path)


def watch_raw_directory(
    path: str | Path,
    db_path: str | Path,
):
    """Watch a directory for new RAW files."""

    raw_directory = Path(path).resolve()

    initialize_ledger(db_path)

    # Reconcile files that already exist when the watcher starts
    existing_raw_files = list(raw_directory.glob("*.raw"))

    for raw_path in existing_raw_files:
        register_raw_file(db_path, raw_path)

    # Wake raw2Sv once after reconciliation.
    if existing_raw_files:
        emit_raw_update_event(raw_directory)

    return watch_directory(
        directory=raw_directory,
        callback=lambda raw_path: register_and_emit_raw_update(raw_path, db_path),
        pattern="*.raw",
    )