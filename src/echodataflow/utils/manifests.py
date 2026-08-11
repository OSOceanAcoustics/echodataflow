"""Utilities for reading, writing, and filtering processing manifests."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from echodataflow.operations.operations_postprocessing import PlannedSlice

RAW_COLUMNS = ["s3_path", "timestamp", "raw_filename", "status", "error"]
REALTIME_SV_COLUMNS = [
    "raw_filename",
    "Sv_filename",
    "first_ping_time",
    "last_ping_time",
]
REALTIME_MVBS_COLUMNS = ["MVBS_filename", "first_ping_time", "last_ping_time"]
REALTIME_PREDICTION_COLUMNS = [
    "prediction_filename_postfix",
    "score_filename",
    "softmax_filename",
    "prediction_filename",
    "evr_filename",
    "first_ping_time",
    "last_ping_time",
]
SV_COLUMNS = [
    "s3_path",
    "raw_filename",
    "Sv_filename",
    "first_ping_time",
    "last_ping_time",
]
MVBS_COLUMNS = [
    "MVBS_filename",
    "slice_start",
    "slice_end",
    "first_ping_time",
    "last_ping_time",
    "is_partial",
]
PREDICTION_COLUMNS = [
    "prediction_filename_postfix",
    "score_filename",
    "softmax_filename",
    "prediction_filename",
    "evr_filename",
    "slice_start",
    "slice_end",
    "first_ping_time",
    "last_ping_time",
]


def read_manifest(path: Path, columns: list[str], date_columns: list[str]) -> pd.DataFrame:
    # Return a schema-correct empty manifest on the first run
    if not path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(path, index_col=0)
    # Add newly introduced columns while preserving older manifest files
    for column in columns:
        if column not in frame:
            frame[column] = pd.NA
    frame = frame[columns]
    for column in date_columns:
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], utc=True)
    return frame


def write_manifest(frame: pd.DataFrame, path: Path) -> None:
    """Replace a manifest atomically; callers must still enforce one writer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Never expose a partially written CSV to a polling downstream flow
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temporary, date_format="%Y-%m-%dT%H:%M:%S.%f%z")
    temporary.replace(path)


def filter_time_range(
    frame: pd.DataFrame,
    column: str,
    start_time: str | None,
    end_time: str | None,
    include_boundary_neighbors: bool = False,
) -> pd.DataFrame:
    # Treat requested ranges as half-open: [start_time, end_time)
    ordered = frame.sort_values(column)
    selected = ordered
    if start_time is not None:
        selected = selected[selected[column] >= pd.to_datetime(start_time, utc=True)]
    if end_time is not None:
        selected = selected[selected[column] < pd.to_datetime(end_time, utc=True)]

    if include_boundary_neighbors:
        neighbors = []
        if start_time is not None:
            # Include the last file starting before the requested interval
            before = ordered[ordered[column] < pd.to_datetime(start_time, utc=True)].tail(1)
            neighbors.append(before)
        if end_time is not None:
            # Include the first file starting at or after the requested interval
            after = ordered[ordered[column] >= pd.to_datetime(end_time, utc=True)].head(1)
            neighbors.append(after)
        selected = pd.concat([selected, *neighbors]).drop_duplicates()

    return selected.sort_values(column)


def filter_slices(
    slices: list[PlannedSlice],
    start_time: str | None,
    end_time: str | None,
) -> list[PlannedSlice]:
    # Require complete slice containment within optional user bounds
    lower = pd.to_datetime(start_time, utc=True) if start_time else None
    upper = pd.to_datetime(end_time, utc=True) if end_time else None
    return [
        item
        for item in slices
        if (lower is None or item.start_time >= lower) and (upper is None or item.end_time <= upper)
    ]
