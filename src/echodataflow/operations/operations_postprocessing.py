"""Pure planning helpers for batch acoustic post-processing."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class PlannedSlice:
    """A time window and the files that overlap it."""

    start_time: pd.Timestamp
    end_time: pd.Timestamp
    filenames: tuple[str, ...]
    is_partial: bool = False


@dataclass(frozen=True)
class TimeWindow:
    """One aligned half-open time interval."""

    start_time: pd.Timestamp
    end_time: pd.Timestamp


def _utc(value) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True)


def _interval_floor(value, minutes: int) -> pd.Timestamp:
    if minutes <= 0:
        raise ValueError("slice length must be greater than zero")
    return _utc(value).floor(f"{minutes}min")


def generate_aligned_windows(
    first_time,
    last_time,
    window_mins: int,
) -> list[TimeWindow]:
    """Generate complete UTC-aligned windows between two timestamps."""
    duration = pd.Timedelta(minutes=window_mins)
    start = _interval_floor(first_time, window_mins)
    final_end = _interval_floor(last_time, window_mins)

    windows: list[TimeWindow] = []
    while start + duration <= final_end:
        windows.append(TimeWindow(start_time=start, end_time=start + duration))
        start += duration
    return windows


def select_overlapping_records(
    records: pd.DataFrame,
    window: TimeWindow,
    start_column: str,
    end_column: str,
) -> pd.DataFrame:
    """Select records whose time coverage overlaps a window."""
    return records[
        (records[end_column] >= window.start_time) & (records[start_column] < window.end_time)
    ]


def select_contained_records(
    records: pd.DataFrame,
    window: TimeWindow,
    start_column: str,
    end_column: str,
) -> pd.DataFrame:
    """Select records fully contained within a window."""
    return records[
        (records[start_column] >= window.start_time) & (records[end_column] <= window.end_time)
    ]


def plan_mvbs_slices(
    raw_processing: pd.DataFrame,
    sv_files: pd.DataFrame,
    slice_mins: int = 20,
) -> list[PlannedSlice]:
    """Return MVBS slices whose registered raw inputs are complete.

    A slice is sealed only after the completion watermark has reached its end.
    Failed or pending raw files whose source timestamps fall in the slice keep it
    from becoming ready.
    """
    if raw_processing.empty or sv_files.empty:
        return []

    raw = raw_processing.copy()
    sv = sv_files.copy()
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
    sv["first_ping_time"] = pd.to_datetime(sv["first_ping_time"], utc=True)
    sv["last_ping_time"] = pd.to_datetime(sv["last_ping_time"], utc=True)

    completed = raw[raw["status"] == "completed"]
    if completed.empty:
        return []

    # The watermark seals only intervals ending before the latest completed input
    completed_keys = set(completed["s3_path"].astype(str))
    watermark = completed["timestamp"].max()
    coverage_start = sv["first_ping_time"].min()
    coverage_end = sv["last_ping_time"].max()

    slices: list[PlannedSlice] = []
    windows = generate_aligned_windows(
        sv["first_ping_time"].min(),
        watermark,
        slice_mins,
    )
    for window in windows:
        # Include the preceding raw file because it may cross the slice boundary
        expected = raw[
            (raw["timestamp"] >= window.start_time) & (raw["timestamp"] < window.end_time)
        ]
        predecessor = raw[raw["timestamp"] < window.start_time].sort_values("timestamp").tail(1)
        if not predecessor.empty:
            expected = pd.concat([predecessor, expected], ignore_index=True)
        expected_keys = set(expected["s3_path"].astype(str))
        if expected_keys and expected_keys.issubset(completed_keys):
            # Pass every converted Sv store that overlaps the half-open slice
            overlapping = select_overlapping_records(
                sv,
                window,
                "first_ping_time",
                "last_ping_time",
            ).sort_values("first_ping_time")
            if not overlapping.empty:
                slices.append(
                    PlannedSlice(
                        start_time=window.start_time,
                        end_time=window.end_time,
                        filenames=tuple(overlapping["Sv_filename"].astype(str)),
                        is_partial=(
                            coverage_start > window.start_time or coverage_end < window.end_time
                        ),
                    )
                )
    return slices


def plan_prediction_slices(
    mvbs_files: pd.DataFrame,
    mvbs_slice_mins: int = 20,
    prediction_slice_mins: int = 40,
    require_complete_window: bool = True,
) -> list[PlannedSlice]:
    """Group completed MVBS slices into aligned prediction windows."""
    if prediction_slice_mins % mvbs_slice_mins != 0:
        raise ValueError("prediction_slice_mins must be a multiple of mvbs_slice_mins")
    if mvbs_files.empty:
        return []

    mvbs = mvbs_files.copy()
    mvbs["slice_start"] = pd.to_datetime(mvbs["slice_start"], utc=True)
    mvbs["slice_end"] = pd.to_datetime(mvbs["slice_end"], utc=True)
    expected_count = prediction_slice_mins // mvbs_slice_mins

    slices: list[PlannedSlice] = []
    # Align every prediction window to a fixed UTC interval boundary
    windows = generate_aligned_windows(
        mvbs["slice_start"].min(),
        mvbs["slice_end"].max(),
        prediction_slice_mins,
    )
    for window in windows:
        # Select the smaller MVBS slices fully contained in this prediction window
        contributing = select_contained_records(
            mvbs,
            window,
            "slice_start",
            "slice_end",
        ).sort_values("slice_start")
        starts = set(contributing["slice_start"])
        expected_starts = {
            window.start_time + pd.Timedelta(minutes=mvbs_slice_mins * n)
            for n in range(expected_count)
        }
        # Completeness requires every expected aligned MVBS start exactly once
        complete = len(contributing) == expected_count and starts == expected_starts
        partial = False
        if "is_partial" in contributing:
            partial = (
                contributing["is_partial"]
                .map(lambda value: str(value).strip().lower() in {"true", "1", "yes"})
                .any()
            )
        if not contributing.empty and (not require_complete_window or (complete and not partial)):
            slices.append(
                PlannedSlice(
                    start_time=window.start_time,
                    end_time=window.end_time,
                    filenames=tuple(contributing["MVBS_filename"].astype(str)),
                    is_partial=not complete or bool(partial),
                )
            )
    return slices
