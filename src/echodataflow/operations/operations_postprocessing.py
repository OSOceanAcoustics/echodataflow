"""Pure planning helpers for batch acoustic post-processing."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from echodataflow.utils.manifests import (
    filter_time_range,
    read_manifest,
    write_manifest,
)
from echodataflow.utils.utils import extract_datetime_from_filename


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


def generate_aligned_windows(
    first_time,
    last_time,
    window_mins: int,
) -> list[TimeWindow]:
    """Generate complete UTC-aligned windows between two timestamps."""
    if window_mins <= 0:
        raise ValueError("slice length must be greater than zero")
    duration = pd.Timedelta(minutes=window_mins)
    start = pd.to_datetime(first_time, utc=True).floor(f"{window_mins}min")
    final_end = pd.to_datetime(last_time, utc=True).floor(f"{window_mins}min")

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


def build_sv_ledger(raw_files: pd.DataFrame) -> pd.DataFrame:
    """Build the fixed post-processing Sv ledger from the complete raw list."""
    required = {"s3_path"}
    if not required.issubset(raw_files.columns):
        raise ValueError(f"raw list must contain columns: {sorted(required)}")

    ledger = raw_files[["s3_path"]].copy()
    ledger["s3_path"] = ledger["s3_path"].astype(str)
    ledger["raw_filename"] = ledger["s3_path"].map(lambda value: Path(value).name)
    if ledger["s3_path"].duplicated().any():
        raise ValueError("raw list contains duplicate S3 paths")
    if ledger["raw_filename"].duplicated().any():
        raise ValueError("raw list contains duplicate basenames")

    ledger["timestamp"] = ledger["raw_filename"].map(extract_datetime_from_filename)
    invalid = ledger.loc[ledger["timestamp"].isna(), "raw_filename"].tolist()
    if invalid:
        raise ValueError(f"Could not parse timestamps from raw filenames: {invalid}")
    ledger["timestamp"] = pd.to_datetime(ledger["timestamp"], utc=True)

    ledger["Sv_filename"] = pd.NA
    ledger["raw2Sv_status"] = "pending"
    ledger["first_ping_time"] = pd.NaT
    ledger["last_ping_time"] = pd.NaT
    ledger["error"] = ""
    return ledger.sort_values("timestamp").reset_index(drop=True)


def build_mvbs_ledger(sv_ledger: pd.DataFrame, slice_mins: int = 20) -> pd.DataFrame:
    """Preplan every MVBS slice and its required raw files."""
    if sv_ledger.empty:
        return pd.DataFrame()

    df_Sv = sv_ledger.copy()
    df_Sv["timestamp"] = pd.to_datetime(df_Sv["timestamp"], utc=True)
    duration = pd.Timedelta(minutes=slice_mins)
    final_end = df_Sv["timestamp"].max().floor(f"{slice_mins}min") + duration
    windows = generate_aligned_windows(
        df_Sv["timestamp"].min(),
        final_end,
        slice_mins,
    )

    records = []
    for window in windows:
        # Record the raw files required by this slice, including its predecessor
        required = filter_time_range(
            df=df_Sv,
            column_start_time="timestamp",
            column_end_time=None,
            start_time=window.start_time,
            end_time=window.end_time,
        )
        records.append(
            {
                "MVBS_filename": f"MVBS_{window.start_time:%Y%m%dT%H%M%S}.zarr",
                "slice_start": window.start_time,
                "slice_end": window.end_time,
                "raw_filenames": json.dumps(required["raw_filename"].tolist()),
                "first_ping_time": pd.NaT,
                "last_ping_time": pd.NaT,
                "is_partial": pd.NA,
                "MVBS_status": "pending",
                "error": "",
            }
        )
    return pd.DataFrame.from_records(records)


def build_prediction_ledger(
    mvbs_ledger: pd.DataFrame,
    mvbs_slice_mins: int = 20,
    prediction_slice_mins: int = 40,
) -> pd.DataFrame:
    """Preplan every prediction window and its required MVBS slices."""
    if mvbs_ledger.empty:
        return pd.DataFrame()

    df_MVBS = mvbs_ledger.copy()
    df_MVBS["slice_start"] = pd.to_datetime(df_MVBS["slice_start"], utc=True)
    df_MVBS["slice_end"] = pd.to_datetime(df_MVBS["slice_end"], utc=True)
    windows = generate_aligned_windows(
        df_MVBS["slice_start"].min(),
        df_MVBS["slice_end"].max(),
        prediction_slice_mins,
    )

    records = []
    for window in windows:
        required = filter_time_range(
            df=df_MVBS,
            column_start_time="slice_start",
            column_end_time="slice_end",
            start_time=window.start_time,
            end_time=window.end_time,
            include_exact_start_time=False,
        )
        records.append(
            {
                "prediction_filename_postfix": window.start_time.strftime("%Y%m%dT%H%M%S"),
                "slice_start": window.start_time,
                "slice_end": window.end_time,
                "MVBS_filenames": json.dumps(required["MVBS_filename"].tolist()),
                "score_filename": pd.NA,
                "softmax_filename": pd.NA,
                "prediction_filename": pd.NA,
                "evr_filename": pd.NA,
                "first_ping_time": pd.NaT,
                "last_ping_time": pd.NaT,
                "prediction_status": "pending",
                "error": "",
            }
        )
    return pd.DataFrame.from_records(records)


def read_or_create_ledger(
    ledger_path: Path,
    columns: list[str],
    date_columns: list[str],
    builder: Callable[[], pd.DataFrame],
) -> pd.DataFrame:
    """Load a ledger or create it with the supplied builder."""
    if ledger_path.exists():
        return read_manifest(path=ledger_path, columns=columns, date_columns=date_columns)

    ledger = builder()
    write_manifest(ledger, ledger_path)
    return ledger


def plan_mvbs_slices(
    sv_ledger: pd.DataFrame,
    mvbs_ledger: pd.DataFrame,
) -> list[PlannedSlice]:
    """Return pending MVBS slices whose required raw conversions are complete."""
    if sv_ledger.empty or mvbs_ledger.empty:
        return []

    df_Sv = sv_ledger.copy()
    df_MVBS = mvbs_ledger.copy()

    # Ensure datetime columns are in UTC
    df_Sv["first_ping_time"] = pd.to_datetime(df_Sv["first_ping_time"], utc=True)
    df_Sv["last_ping_time"] = pd.to_datetime(df_Sv["last_ping_time"], utc=True)
    df_MVBS["slice_start"] = pd.to_datetime(df_MVBS["slice_start"], utc=True)
    df_MVBS["slice_end"] = pd.to_datetime(df_MVBS["slice_end"], utc=True)

    slices: list[PlannedSlice] = []
    for row in df_MVBS.itertuples(index=False):
        # Skip slices that have already been created successfully
        if row.MVBS_status in {"completed", "no_data"}:
            continue

        required_raw = json.loads(row.raw_filenames)
        required_Sv = df_Sv[df_Sv["raw_filename"].isin(required_raw)]
        if len(required_Sv) != len(required_raw):
            raise ValueError(f"MVBS ledger references unknown raw files: {required_raw}")

        # Skip if any required Sv conversions are not yet completed
        if not required_Sv["raw2Sv_status"].eq("completed").all():
            continue

        slices.append(
            PlannedSlice(
                start_time=row.slice_start,
                end_time=row.slice_end,
                filenames=tuple(required_Sv.sort_values("timestamp")["Sv_filename"].astype(str)),
                is_partial=(
                    required_Sv["first_ping_time"].min() > row.slice_start
                    or required_Sv["last_ping_time"].max() < row.slice_end
                ),
            )
        )
    return slices


def plan_prediction_slices(
    mvbs_ledger: pd.DataFrame,
    prediction_ledger: pd.DataFrame,
) -> list[PlannedSlice]:
    """Return pending prediction windows whose required MVBS slices are ready."""
    if mvbs_ledger.empty or prediction_ledger.empty:
        return []

    df_MVBS = mvbs_ledger.copy()
    df_prediction = prediction_ledger.copy()
    df_prediction["slice_start"] = pd.to_datetime(df_prediction["slice_start"], utc=True)
    df_prediction["slice_end"] = pd.to_datetime(df_prediction["slice_end"], utc=True)

    slices: list[PlannedSlice] = []
    for row in df_prediction.itertuples(index=False):
        if row.prediction_status == "completed":
            continue

        required_names = json.loads(row.MVBS_filenames)
        required_MVBS = df_MVBS[df_MVBS["MVBS_filename"].isin(required_names)]
        if len(required_MVBS) != len(required_names):
            raise ValueError(f"Prediction ledger references unknown MVBS files: {required_names}")

        if not required_MVBS["MVBS_status"].eq("completed").all():
            continue

        partial = (
            required_MVBS["is_partial"]
            .map(lambda value: str(value).strip().lower() in {"true", "1", "yes"})
            .any()
        )

        slices.append(
            PlannedSlice(
                start_time=row.slice_start,
                end_time=row.slice_end,
                filenames=tuple(
                    required_MVBS.sort_values("slice_start")["MVBS_filename"].astype(str)
                ),
                is_partial=bool(partial),
            )
        )
    return slices
