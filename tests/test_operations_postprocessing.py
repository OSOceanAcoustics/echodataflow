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
    assert len(planned[0].input_signature) == 64


def test_mvbs_signature_changes_when_sv_ping_bounds_change():
    original = plan_mvbs_slices(_raw_manifest(), _sv_manifest(), slice_mins=20)[0]
    changed_sv = _sv_manifest()
    changed_sv.loc[0, "last_ping_time"] = pd.Timestamp("2025-06-11T00:07:01Z")

    changed = plan_mvbs_slices(_raw_manifest(), changed_sv, slice_mins=20)[0]

    assert changed.filenames == original.filenames
    assert changed.input_signature != original.input_signature


def test_mvbs_signature_changes_when_an_sv_file_is_added():
    original = plan_mvbs_slices(_raw_manifest(), _sv_manifest(), slice_mins=20)[0]
    changed_raw = pd.concat(
        [
            _raw_manifest(),
            pd.DataFrame(
                {
                    "s3_path": ["late.raw"],
                    "timestamp": pd.to_datetime(["2025-06-11T00:18:00Z"]),
                    "status": ["completed"],
                }
            ),
        ],
        ignore_index=True,
    )
    changed_sv = pd.concat(
        [
            _sv_manifest(),
            pd.DataFrame(
                {
                    "s3_path": ["late.raw"],
                    "Sv_filename": ["late.zarr"],
                    "first_ping_time": pd.to_datetime(["2025-06-11T00:18:00Z"]),
                    "last_ping_time": pd.to_datetime(["2025-06-11T00:19:00Z"]),
                }
            ),
        ],
        ignore_index=True,
    )

    changed = plan_mvbs_slices(changed_raw, changed_sv, slice_mins=20)[0]

    assert changed.filenames == ("a.zarr", "b.zarr", "c.zarr", "late.zarr")
    assert changed.input_signature != original.input_signature


def test_pending_raw_input_keeps_its_mvbs_slice_closed():
    raw = _raw_manifest(("completed", "completed", "pending", "completed"))

    assert plan_mvbs_slices(raw, _sv_manifest(), slice_mins=20) == []


def test_prediction_combines_two_aligned_mvbs_slices():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["first.zarr", "second.zarr"],
            "first_ping_time": pd.to_datetime(["2025-06-11T00:00:05Z", "2025-06-11T00:20:05Z"]),
            "last_ping_time": pd.to_datetime(["2025-06-11T00:19:55Z", "2025-06-11T00:39:55Z"]),
            "is_partial": [False, False],
            "input_signature": ["sv-signature-1", "sv-signature-2"],
        }
    )

    planned = plan_prediction_slices(mvbs, 20, 40)

    assert len(planned) == 1
    assert planned[0].start_time == pd.Timestamp("2025-06-11T00:00:00Z")
    assert planned[0].end_time == pd.Timestamp("2025-06-11T00:40:00Z")
    assert planned[0].filenames == ("first.zarr", "second.zarr")
    assert len(planned[0].input_signature) == 64


def test_prediction_signature_changes_when_mvbs_ping_bounds_change():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["first.zarr", "second.zarr"],
            "first_ping_time": pd.to_datetime(["2025-06-11T00:00:05Z", "2025-06-11T00:20:05Z"]),
            "last_ping_time": pd.to_datetime(["2025-06-11T00:19:55Z", "2025-06-11T00:39:55Z"]),
            "is_partial": [False, False],
        }
    )
    original = plan_prediction_slices(mvbs, 20, 40)[0]
    mvbs.loc[1, "first_ping_time"] = pd.Timestamp("2025-06-11T00:20:10Z")

    changed = plan_prediction_slices(mvbs, 20, 40)[0]

    assert changed.input_signature != original.input_signature


def test_prediction_signature_propagates_changed_mvbs_signature():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["first.zarr", "second.zarr"],
            "first_ping_time": pd.to_datetime(["2025-06-11T00:00:05Z", "2025-06-11T00:20:05Z"]),
            "last_ping_time": pd.to_datetime(["2025-06-11T00:19:55Z", "2025-06-11T00:39:55Z"]),
            "is_partial": [False, False],
            "input_signature": ["original-1", "original-2"],
        }
    )
    original = plan_prediction_slices(mvbs, 20, 40)[0]
    mvbs.loc[1, "input_signature"] = "changed-2"

    changed = plan_prediction_slices(mvbs, 20, 40)[0]

    assert changed.input_signature != original.input_signature


def test_prediction_requires_both_mvbs_slices_by_default():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["first.zarr"],
            "first_ping_time": pd.to_datetime(["2025-06-11T00:00:05Z"]),
            "last_ping_time": pd.to_datetime(["2025-06-11T00:19:55Z"]),
            "is_partial": [False],
        }
    )

    assert plan_prediction_slices(mvbs, 20, 40) == []


def test_incomplete_prediction_uses_any_actual_ping_time_overlap_when_allowed():
    mvbs = pd.DataFrame(
        {
            "MVBS_filename": ["overlapping.zarr", "outside.zarr"],
            "first_ping_time": pd.to_datetime(["2025-06-11T00:05:00Z", "2025-06-11T00:40:00Z"]),
            "last_ping_time": pd.to_datetime(["2025-06-11T00:15:00Z", "2025-06-11T00:45:00Z"]),
            "is_partial": [True, True],
        }
    )

    planned = plan_prediction_slices(
        mvbs,
        20,
        40,
        require_complete_window=False,
    )

    assert len(planned) == 2
    assert planned[0].filenames == ("overlapping.zarr",)
    assert planned[1].filenames == ("outside.zarr",)


def test_prediction_length_must_align_with_mvbs_length():
    with pytest.raises(ValueError, match="must be a multiple"):
        plan_prediction_slices(pd.DataFrame(), 20, 30)
