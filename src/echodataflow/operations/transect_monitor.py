from pathlib import Path

from prefect.events import emit_event

from echodataflow.utils.file_watcher import watch_file


TRANSECT_UPDATE_EVENT = "echodataflow.transect.updated"
TRANSECT_RESOURCE_ID = "transect-start-end-time"
TRANSECT_RELATED_RESOURCE_ID = "transect-monitor"


def emit_transect_update_event(path: Path) -> None:
    """Emit a Prefect event when the transect CSV is updated."""

    emit_event(
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


def watch_transect_file(path: str | Path):
    """Watch the transect start/end CSV and emit a Prefect event on update."""

    return watch_file(
        target_file=path,
        callback=emit_transect_update_event,
    )
