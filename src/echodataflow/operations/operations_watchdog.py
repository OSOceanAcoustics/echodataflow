from pathlib import Path

from prefect.events import emit_event
from prefect.events.worker import EventsWorker
from watchdog.observers import Observer

from echodataflow.utils.file_watcher import watch_directory, watch_file
from echodataflow.utils.processing_ledger import (
    initialize_ledger,
    register_raw_file,
)


RAW_UPDATE_EVENT = "echodataflow.raw.updated"
RAW_RESOURCE_ID = "raw-monitor"

TRANSECT_UPDATE_EVENT = "echodataflow.transect.updated"
TRANSECT_RESOURCE_ID = "transect-start-end-time"
TRANSECT_RELATED_RESOURCE_ID = "transect-monitor"


def _flush_events() -> None:
    """Wait until queued Prefect events have been sent."""
    EventsWorker.instance().wait_until_empty()


def emit_raw_update_event(path: Path) -> None:
    """Emit a Prefect event when a RAW file arrives."""

    event = emit_event(
        event=RAW_UPDATE_EVENT,
        resource={
            "prefect.resource.id": RAW_RESOURCE_ID,
            "prefect.resource.name": RAW_RESOURCE_ID,
            "path": str(path),
        },
    )

    print(f"RAW event emitted for {path}: {event}")

    if event is not None:
        _flush_events()
        print("RAW event queue flushed")


def register_and_emit_raw_update(
    path: Path,
    db_path: str | Path,
) -> None:
    """Emit a Prefect event only when registration changes the ledger."""
    if register_raw_file(db_path, path):  # emit only if ledger is udpated
        emit_raw_update_event(path)


def watch_raw_directory(
    path: str | Path,
    db_path: str | Path,
) -> Observer:
    """Watch a directory for new RAW files and returning a running observer."""

    raw_directory = Path(path).resolve()

    initialize_ledger(db_path)

    # Reconcile files that already exist when the watcher starts
    existing_raw_files = list(raw_directory.glob("*.raw"))

    for raw_path in existing_raw_files:
        register_raw_file(db_path, raw_path)

    # Wake raw2Sv once after reconciliation
    if existing_raw_files:
        emit_raw_update_event(raw_directory)

    return watch_directory(  # this returns a running observer
        directory=raw_directory,
        callback=lambda raw_path: register_and_emit_raw_update(
            raw_path,
            db_path,
        ),
        pattern="*.raw",
    )


def emit_transect_update_event(path: Path) -> None:
    """Emit a Prefect event when the transect CSV is updated."""

    event = emit_event(
        event=TRANSECT_UPDATE_EVENT,
        resource={
            "prefect.resource.id": TRANSECT_RESOURCE_ID,
            "path": str(path),
        },
        related=[
            {
                "prefect.resource.id": TRANSECT_RELATED_RESOURCE_ID,
                "prefect.resource.name": TRANSECT_RELATED_RESOURCE_ID,
                "prefect.resource.role": "deployment",
            }
        ],
    )

    if event is not None:
        _flush_events()


def watch_transect_file(path: str | Path):
    """Watch the transect start/end CSV and emit a Prefect event on update."""

    return watch_file(
        target_file=path,
        callback=emit_transect_update_event,
    )
