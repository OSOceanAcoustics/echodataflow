"""Pure planning helpers for batch acoustic post-processing."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class PlannedSlice:
    """A time window and the files that overlap it."""

    start_time: pd.Timestamp
    end_time: pd.Timestamp
    filenames: tuple[str, ...]
    is_partial: bool = False
    input_signature: str = ""


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


def build_input_signature(
    records: pd.DataFrame,
    columns: list[str],
    window: TimeWindow,
) -> str:
    """Create a deterministic fingerprint of window inputs and their metadata."""

    def fix_value(value):
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        return str(value)

    rows = [
        {column: fix_value(row[column]) for column in columns}
        for _, row in records.sort_values(columns[0]).iterrows()
    ]
    payload = {
        "window_start": window.start_time.isoformat(),
        "window_end": window.end_time.isoformat(),
        "inputs": rows,
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()


def plan_mvbs_slices(
    df_raw_in: pd.DataFrame,
    df_Sv_in: pd.DataFrame,
    slice_mins: int = 20,
) -> list[PlannedSlice]:
    """Return MVBS slices whose registered raw inputs are complete.

    A slice is sealed only after the completion watermark has reached its end.
    Failed or pending raw files whose source timestamps fall in the slice keep it
    from becoming ready.
    """
    if df_raw_in.empty or df_Sv_in.empty:
        return []

    df_raw = df_raw_in.copy()
    df_Sv = df_Sv_in.copy()
    df_raw["timestamp"] = pd.to_datetime(df_raw["timestamp"], utc=True)
    df_Sv["first_ping_time"] = pd.to_datetime(df_Sv["first_ping_time"], utc=True)
    df_Sv["last_ping_time"] = pd.to_datetime(df_Sv["last_ping_time"], utc=True)

    df_completed = df_raw[df_raw["status"] == "completed"]
    if df_completed.empty:
        return []

    # The watermark seals only intervals ending before the latest completed input
    completed_keys = set(df_completed["s3_path"].astype(str))
    watermark = df_completed["timestamp"].max()
    coverage_start = df_Sv["first_ping_time"].min()
    coverage_end = df_Sv["last_ping_time"].max()

    slices: list[PlannedSlice] = []
    windows = generate_aligned_windows(
        df_Sv["first_ping_time"].min(),
        watermark,
        slice_mins,
    )
    for window in windows:
        # Include the preceding raw file because it may cross the slice boundary
        expected = df_raw[
            (df_raw["timestamp"] >= window.start_time) & (df_raw["timestamp"] < window.end_time)
        ]
        predecessor = df_raw[df_raw["timestamp"] < window.start_time].sort_values("timestamp").tail(1)
        if not predecessor.empty:
            expected = pd.concat([predecessor, expected], ignore_index=True)
        expected_keys = set(expected["s3_path"].astype(str))
        if expected_keys and expected_keys.issubset(completed_keys):
            # Pass every converted Sv store that overlaps the half-open slice
            overlapping = select_overlapping_records(
                df_Sv,
                window,
                "first_ping_time",
                "last_ping_time",
            ).sort_values("first_ping_time")
            if not overlapping.empty:
                input_signature = build_input_signature(
                    overlapping,
                    ["Sv_filename", "first_ping_time", "last_ping_time"],
                    window,
                )
                slices.append(
                    PlannedSlice(
                        start_time=window.start_time,
                        end_time=window.end_time,
                        filenames=tuple(overlapping["Sv_filename"].astype(str)),
                        is_partial=(
                            coverage_start > window.start_time or coverage_end < window.end_time
                        ),
                        input_signature=input_signature,
                    )
                )
    return slices


def plan_prediction_slices(
    df_MVBS_in: pd.DataFrame,
    mvbs_slice_mins: int = 20,
    prediction_slice_mins: int = 40,
    require_complete_window: bool = True,
) -> list[PlannedSlice]:
    """Group completed MVBS slices into aligned prediction windows."""
    if prediction_slice_mins % mvbs_slice_mins != 0:
        raise ValueError("prediction_slice_mins must be a multiple of mvbs_slice_mins")
    if df_MVBS_in.empty:
        return []

    df_MVBS = df_MVBS_in.copy()
    df_MVBS["first_ping_time"] = pd.to_datetime(df_MVBS["first_ping_time"], utc=True)
    df_MVBS["last_ping_time"] = pd.to_datetime(df_MVBS["last_ping_time"], utc=True)
    expected_count = prediction_slice_mins // mvbs_slice_mins

    slices: list[PlannedSlice] = []
    # Derive prediction windows directly from the available ping-time coverage
    windows = generate_aligned_windows(
        df_MVBS["first_ping_time"].min(),
        df_MVBS["last_ping_time"].max().ceil(f"{prediction_slice_mins}min"),
        prediction_slice_mins,
    )
    for window in windows:
        # Match the real-time flow's actual ping-time overlap selection
        contributing = select_overlapping_records(
            df_MVBS,
            window,
            "first_ping_time",
            "last_ping_time",
        ).sort_values("first_ping_time")
        # Optional completeness requires the expected number of non-partial inputs
        complete = len(contributing) == expected_count
        partial = False
        if "is_partial" in contributing:
            partial = (
                contributing["is_partial"]
                .map(lambda value: str(value).strip().lower() in {"true", "1", "yes"})
                .any()
            )
        if not contributing.empty and (not require_complete_window or (complete and not partial)):
            signature_columns = ["MVBS_filename", "first_ping_time", "last_ping_time"]
            if "input_signature" in contributing:
                # Propagate upstream Sv changes even when MVBS ping bounds stay fixed
                signature_columns.append("input_signature")
            input_signature = build_input_signature(
                contributing,
                signature_columns,
                window,
            )
            slices.append(
                PlannedSlice(
                    start_time=window.start_time,
                    end_time=window.end_time,
                    filenames=tuple(contributing["MVBS_filename"].astype(str)),
                    is_partial=not complete or bool(partial),
                    input_signature=input_signature,
                )
            )
    return slices
