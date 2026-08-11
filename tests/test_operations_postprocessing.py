import pandas as pd
import pytest

from echodataflow.operations.operations_postprocessing import (
    TimeWindow,
    generate_aligned_windows,
    plan_mvbs_slices,
    plan_prediction_slices,
    select_contained_records,
    select_overlapping_records,
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


def _raw_manifest(statuses=("completed", "completed", "completed", "completed")):
    return pd.DataFrame(
        {
            "s3_path": ["a.raw", "b.raw", "c.raw", "d.raw"],
            "timestamp": pd.to_datetime(
                [
                    "2025-06-11T00:01:00Z",
                    "2025-06-11T00:07:00Z",
                    "2025-06-11T00:14:00Z",
                    "2025-06-11T00:21:00Z",
                ]
            ),
            "status": statuses,
        }
    )


def _sv_manifest():
    return pd.DataFrame(
        {
            "s3_path": ["a.raw", "b.raw", "c.raw", "d.raw"],
            "Sv_filename": ["a.zarr", "b.zarr", "c.zarr", "d.zarr"],
            "first_ping_time": pd.to_datetime(
                [
                    "2025-06-11T00:01:00Z",
                    "2025-06-11T00:07:00Z",
                    "2025-06-11T00:14:00Z",
                    "2025-06-11T00:21:00Z",
                ]
            ),
            "last_ping_time": pd.to_datetime(
                [
                    "2025-06-11T00:06:59Z",
                    "2025-06-11T00:13:59Z",
                    "2025-06-11T00:20:59Z",
                    "2025-06-11T00:27:00Z",
                ]
            ),
        }
    )


def test_mvbs_slice_is_released_after_watermark_passes_end():
    planned = plan_mvbs_slices(_raw_manifest(), _sv_manifest(), slice_mins=20)

    assert len(planned) == 1
    assert planned[0].start_time == pd.Timestamp("2025-06-11T00:00:00Z")
    assert planned[0].end_time == pd.Timestamp("2025-06-11T00:20:00Z")
    assert planned[0].filenames == ("a.zarr", "b.zarr", "c.zarr")
    assert planned[0].is_partial


def test_pending_raw_input_keeps_its_mvbs_slice_closed():
    raw = _raw_manifest(("completed", "completed", "pending", "completed"))

    assert plan_mvbs_slices(raw, _sv_manifest(), slice_mins=20) == []


def test_prediction_combines_two_aligned_mvbs_slices():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["first.zarr", "second.zarr"],
            "slice_start": pd.to_datetime(["2025-06-11T00:00:00Z", "2025-06-11T00:20:00Z"]),
            "slice_end": pd.to_datetime(["2025-06-11T00:20:00Z", "2025-06-11T00:40:00Z"]),
            "is_partial": [False, False],
        }
    )

    planned = plan_prediction_slices(mvbs, 20, 40)

    assert len(planned) == 1
    assert planned[0].start_time == pd.Timestamp("2025-06-11T00:00:00Z")
    assert planned[0].end_time == pd.Timestamp("2025-06-11T00:40:00Z")
    assert planned[0].filenames == ("first.zarr", "second.zarr")


def test_prediction_requires_both_mvbs_slices_by_default():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["first.zarr"],
            "slice_start": pd.to_datetime(["2025-06-11T00:00:00Z"]),
            "slice_end": pd.to_datetime(["2025-06-11T00:20:00Z"]),
            "is_partial": [False],
        }
    )

    assert plan_prediction_slices(mvbs, 20, 40) == []


def test_prediction_length_must_align_with_mvbs_length():
    with pytest.raises(ValueError, match="must be a multiple"):
        plan_prediction_slices(pd.DataFrame(), 20, 30)
