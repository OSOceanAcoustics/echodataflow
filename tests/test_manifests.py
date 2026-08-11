import pandas as pd

from echodataflow.utils.manifests import filter_time_range


def test_filter_time_range_can_include_one_file_outside_each_boundary():
    frame = pd.DataFrame(
        {
            "s3_path": ["before.raw", "first.raw", "last.raw", "after.raw", "later.raw"],
            "timestamp": pd.to_datetime(
                [
                    "2025-06-18T23:54:00Z",
                    "2025-06-19T00:02:00Z",
                    "2025-06-19T01:54:00Z",
                    "2025-06-19T02:02:00Z",
                    "2025-06-19T02:10:00Z",
                ]
            ),
        }
    )

    selected = filter_time_range(
        frame,
        "timestamp",
        "2025-06-19T00:00:00Z",
        "2025-06-19T02:00:00Z",
        include_boundary_neighbors=True,
    )

    assert selected["s3_path"].tolist() == [
        "before.raw",
        "first.raw",
        "last.raw",
        "after.raw",
    ]


def test_filter_time_range_remains_half_open_without_neighbors():
    frame = pd.DataFrame(
        {
            "s3_path": ["start.raw", "end.raw"],
            "timestamp": pd.to_datetime(["2025-06-19T00:00:00Z", "2025-06-19T02:00:00Z"]),
        }
    )

    selected = filter_time_range(
        frame,
        "timestamp",
        "2025-06-19T00:00:00Z",
        "2025-06-19T02:00:00Z",
    )

    assert selected["s3_path"].tolist() == ["start.raw"]
