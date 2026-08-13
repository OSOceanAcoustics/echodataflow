import json

import pandas as pd
import pytest

from echodataflow.operations.operations_postprocessing import (
    TimeWindow,
    build_prediction_ledger,
    build_mvbs_ledger,
    build_sv_ledger,
    generate_aligned_windows,
    plan_mvbs_slices,
    plan_prediction_slices,
    select_contained_records,
    select_overlapping_records,
)
from echodataflow.utils.manifests import (
    MVBS_COLUMNS_POSTPROCESSING,
    PREDICTION_COLUMNS_POSTPROCESSING,
)


def test_generate_aligned_windows_returns_only_complete_windows():
    assert generate_aligned_windows(
        "2025-06-11T00:03:00Z",
        "2025-06-11T01:01:00Z",
        20,
    ) == [
        TimeWindow(
            pd.Timestamp("2025-06-11T00:00:00Z"),
            pd.Timestamp("2025-06-11T00:20:00Z"),
        ),
        TimeWindow(
            pd.Timestamp("2025-06-11T00:20:00Z"),
            pd.Timestamp("2025-06-11T00:40:00Z"),
        ),
        TimeWindow(
            pd.Timestamp("2025-06-11T00:40:00Z"),
            pd.Timestamp("2025-06-11T01:00:00Z"),
        ),
    ]


def test_shared_record_selectors_apply_overlap_and_containment_rules():
    records = pd.DataFrame(
        {
            "name": ["crosses-start", "contained", "crosses-end", "outside"],
            "start": pd.to_datetime(
                [
                    "2025-06-10T23:55:00Z",
                    "2025-06-11T00:05:00Z",
                    "2025-06-11T00:15:00Z",
                    "2025-06-11T00:20:00Z",
                ]
            ),
            "end": pd.to_datetime(
                [
                    "2025-06-11T00:05:00Z",
                    "2025-06-11T00:15:00Z",
                    "2025-06-11T00:25:00Z",
                    "2025-06-11T00:30:00Z",
                ]
            ),
        }
    )
    window = TimeWindow(
        pd.Timestamp("2025-06-11T00:00:00Z"),
        pd.Timestamp("2025-06-11T00:20:00Z"),
    )

    overlapping = select_overlapping_records(records, window, "start", "end")
    contained = select_contained_records(records, window, "start", "end")

    assert overlapping["name"].tolist() == [
        "crosses-start",
        "contained",
        "crosses-end",
    ]
    assert contained["name"].tolist() == ["contained"]


def _sv_ledger(statuses=("completed", "completed", "completed", "completed")):
    raw = pd.DataFrame(
        {
            "s3_path": [
                "survey/IWCPS-D20250611-T000100.raw",
                "survey/IWCPS-D20250611-T000700.raw",
                "survey/IWCPS-D20250611-T001400.raw",
                "survey/IWCPS-D20250611-T002100.raw",
            ],
        }
    )
    ledger = build_sv_ledger(raw)
    ledger["Sv_filename"] = ["a.zarr", "b.zarr", "c.zarr", "d.zarr"]
    ledger["raw2Sv_status"] = statuses
    ledger["first_ping_time"] = pd.to_datetime(
        [
            "2025-06-11T00:01:00Z",
            "2025-06-11T00:07:00Z",
            "2025-06-11T00:14:00Z",
            "2025-06-11T00:21:00Z",
        ]
    )
    ledger["last_ping_time"] = pd.to_datetime(
        [
            "2025-06-11T00:06:59Z",
            "2025-06-11T00:13:59Z",
            "2025-06-11T00:20:59Z",
            "2025-06-11T00:27:00Z",
        ]
    )
    return ledger


def test_ledgers_predeclare_raw_files_and_mvbs_slices():
    sv = _sv_ledger(("pending",) * 4)
    mvbs = build_mvbs_ledger(sv, slice_mins=20)

    assert sv["raw_filename"].tolist() == [
        "IWCPS-D20250611-T000100.raw",
        "IWCPS-D20250611-T000700.raw",
        "IWCPS-D20250611-T001400.raw",
        "IWCPS-D20250611-T002100.raw",
    ]
    assert mvbs["slice_start"].tolist() == [
        pd.Timestamp("2025-06-11T00:00:00Z"),
        pd.Timestamp("2025-06-11T00:20:00Z"),
    ]
    assert mvbs["raw_filenames"].tolist() == [
        '["IWCPS-D20250611-T000100.raw", "IWCPS-D20250611-T000700.raw", '
        '"IWCPS-D20250611-T001400.raw"]',
        '["IWCPS-D20250611-T001400.raw", "IWCPS-D20250611-T002100.raw"]',
    ]


def test_sv_ledger_derives_timestamp_only_from_s3_path():
    raw = pd.DataFrame(
        {
            "s3_path": ["survey/IWCPS-D20250611-T000100.raw"],
            "timestamp": ["1999-01-01T00:00:00Z"],
        }
    )

    ledger = build_sv_ledger(raw)

    assert ledger.loc[0, "timestamp"] == pd.Timestamp("2025-06-11T00:01:00Z")


def test_mvbs_slice_is_released_when_all_required_raw_files_complete():
    sv = _sv_ledger()
    planned = plan_mvbs_slices(sv, build_mvbs_ledger(sv, slice_mins=20))

    assert len(planned) == 2
    assert planned[0].start_time == pd.Timestamp("2025-06-11T00:00:00Z")
    assert planned[0].end_time == pd.Timestamp("2025-06-11T00:20:00Z")
    assert planned[0].filenames == ("a.zarr", "b.zarr", "c.zarr")
    assert planned[0].is_partial


def test_pending_raw_input_keeps_its_mvbs_slice_closed():
    sv = _sv_ledger(("completed", "completed", "pending", "completed"))

    assert plan_mvbs_slices(sv, build_mvbs_ledger(sv, slice_mins=20)) == []


def test_empty_sv_ledger_builds_header_only_mvbs_ledger():
    mvbs = build_mvbs_ledger(pd.DataFrame(), slice_mins=20)

    assert mvbs.empty
    assert mvbs.columns.tolist() == MVBS_COLUMNS_POSTPROCESSING


def test_prediction_combines_two_aligned_mvbs_slices():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["first.zarr", "second.zarr"],
            "slice_start": pd.to_datetime(["2025-06-11T00:00:00Z", "2025-06-11T00:20:00Z"]),
            "slice_end": pd.to_datetime(["2025-06-11T00:20:00Z", "2025-06-11T00:40:00Z"]),
            "first_ping_time": pd.to_datetime(["2025-06-11T00:00:05Z", "2025-06-11T00:20:05Z"]),
            "last_ping_time": pd.to_datetime(["2025-06-11T00:19:55Z", "2025-06-11T00:39:55Z"]),
            "is_partial": [False, False],
            "MVBS_status": ["completed", "completed"],
        }
    )

    prediction = build_prediction_ledger(mvbs, 40)
    planned = plan_prediction_slices(mvbs, prediction)

    assert len(planned) == 1
    assert planned[0].start_time == pd.Timestamp("2025-06-11T00:00:00Z")
    assert planned[0].end_time == pd.Timestamp("2025-06-11T00:40:00Z")
    assert planned[0].filenames == ("first.zarr", "second.zarr")


def test_prediction_planner_skips_completed_prediction_windows():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["first.zarr", "second.zarr"],
            "slice_start": pd.to_datetime(["2025-06-11T00:00:00Z", "2025-06-11T00:20:00Z"]),
            "slice_end": pd.to_datetime(["2025-06-11T00:20:00Z", "2025-06-11T00:40:00Z"]),
            "first_ping_time": pd.to_datetime(["2025-06-11T00:00:05Z", "2025-06-11T00:20:05Z"]),
            "last_ping_time": pd.to_datetime(["2025-06-11T00:19:55Z", "2025-06-11T00:39:55Z"]),
            "is_partial": [False, False],
            "MVBS_status": ["completed", "completed"],
        }
    )
    prediction = build_prediction_ledger(mvbs, 40)
    prediction.loc[0, "prediction_status"] = "completed"

    planned = plan_prediction_slices(mvbs, prediction)

    assert planned == []


def test_prediction_requires_both_mvbs_slices_by_default():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["first.zarr"],
            "slice_start": pd.to_datetime(["2025-06-11T00:00:00Z"]),
            "slice_end": pd.to_datetime(["2025-06-11T00:20:00Z"]),
            "first_ping_time": pd.to_datetime(["2025-06-11T00:00:05Z"]),
            "last_ping_time": pd.to_datetime(["2025-06-11T00:19:55Z"]),
            "is_partial": [False],
            "MVBS_status": ["completed"],
        }
    )

    prediction = build_prediction_ledger(mvbs, 40)

    assert prediction.empty
    assert prediction.columns.tolist() == PREDICTION_COLUMNS_POSTPROCESSING


def test_incomplete_prediction_uses_any_actual_ping_time_overlap_when_allowed():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["overlapping.zarr", "outside.zarr"],
            "slice_start": pd.to_datetime(["2025-06-11T00:00:00Z", "2025-06-11T00:40:00Z"]),
            "slice_end": pd.to_datetime(["2025-06-11T00:20:00Z", "2025-06-11T01:00:00Z"]),
            "first_ping_time": pd.to_datetime(["2025-06-11T00:05:00Z", "2025-06-11T00:40:00Z"]),
            "last_ping_time": pd.to_datetime(["2025-06-11T00:15:00Z", "2025-06-11T00:45:00Z"]),
            "is_partial": [True, True],
            "MVBS_status": ["completed", "completed"],
        }
    )
    prediction = build_prediction_ledger(mvbs, 40)

    planned = plan_prediction_slices(mvbs, prediction)

    assert len(planned) == 1
    assert planned[0].filenames == ("overlapping.zarr",)
    assert planned[0].is_partial


def test_prediction_ledger_supports_overlapping_seven_minute_mvbs_slices():
    starts = pd.date_range("2025-06-11T00:00:00Z", periods=6, freq="7min")
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": [f"slice-{index}.zarr" for index in range(6)],
            "slice_start": starts,
            "slice_end": starts + pd.Timedelta(minutes=7),
        }
    )

    prediction = build_prediction_ledger(mvbs, 40)

    assert json.loads(prediction.loc[0, "MVBS_filenames"]) == [
        f"slice-{index}.zarr" for index in range(6)
    ]
