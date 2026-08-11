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


def _utc(value) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True)


def _interval_floor(value, minutes: int) -> pd.Timestamp:
    if minutes <= 0:
        raise ValueError("slice length must be greater than zero")
    return _utc(value).floor(f"{minutes}min")


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
    duration = pd.Timedelta(minutes=slice_mins)
    first = _interval_floor(sv["first_ping_time"].min(), slice_mins)
    last_end = _interval_floor(watermark, slice_mins)
    coverage_start = sv["first_ping_time"].min()
    coverage_end = sv["last_ping_time"].max()

    slices: list[PlannedSlice] = []
    start = first
    while start + duration <= last_end:
        end = start + duration
        # Include the preceding raw file because it may cross the slice boundary
        expected = raw[(raw["timestamp"] >= start) & (raw["timestamp"] < end)]
        predecessor = raw[raw["timestamp"] < start].sort_values("timestamp").tail(1)
        if not predecessor.empty:
            expected = pd.concat([predecessor, expected], ignore_index=True)
        expected_keys = set(expected["s3_path"].astype(str))
        if expected_keys and expected_keys.issubset(completed_keys):
            # Pass every converted Sv store that overlaps the half-open slice
            overlapping = sv[
                (sv["last_ping_time"] >= start) & (sv["first_ping_time"] < end)
            ].sort_values("first_ping_time")
            if not overlapping.empty:
                slices.append(
                    PlannedSlice(
                        start_time=start,
                        end_time=end,
                        filenames=tuple(overlapping["Sv_filename"].astype(str)),
                        is_partial=coverage_start > start or coverage_end < end,
                    )
                )
        start = end
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
    # Align every prediction window to a fixed UTC interval boundary
    duration = pd.Timedelta(minutes=prediction_slice_mins)
    expected_count = prediction_slice_mins // mvbs_slice_mins
    first = _interval_floor(mvbs["slice_start"].min(), prediction_slice_mins)
    final_end = _interval_floor(mvbs["slice_end"].max(), prediction_slice_mins)

    slices: list[PlannedSlice] = []
    start = first
    while start + duration <= final_end:
        end = start + duration
        # Select the smaller MVBS slices fully contained in this prediction window
        contributing = mvbs[
            (mvbs["slice_start"] >= start) & (mvbs["slice_end"] <= end)
        ].sort_values("slice_start")
        starts = set(contributing["slice_start"])
        expected_starts = {
            start + pd.Timedelta(minutes=mvbs_slice_mins * n) for n in range(expected_count)
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
                    start_time=start,
                    end_time=end,
                    filenames=tuple(contributing["MVBS_filename"].astype(str)),
                    is_partial=not complete or bool(partial),
                )
            )
        start = end
    return slices
