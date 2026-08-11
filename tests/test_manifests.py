from pathlib import Path

import pandas as pd

from echodataflow.utils.manifests import (
    filter_time_range,
    manifest_signature_changed,
    read_manifest,
    write_manifest,
)


def test_manifest_signature_detects_missing_and_changed_outputs():
    manifest = pd.DataFrame(
        {
            "filename": ["output.zarr"],
            "input_signature": ["current"],
        }
    )

    assert not manifest_signature_changed(manifest, "filename", "output.zarr", "current")
    assert manifest_signature_changed(manifest, "filename", "output.zarr", "changed")
    assert manifest_signature_changed(manifest, "filename", "missing.zarr", "current")


def test_manifest_round_trip_repairs_schema_and_normalizes_utc(tmp_path):
    path = tmp_path / "manifest.csv"
    pd.DataFrame(
        {
            "filename": ["example.zarr"],
            "first_ping_time": ["2025-06-19T00:00:00"],
            "legacy_column": ["ignored"],
        }
    ).to_csv(path)

    manifest = read_manifest(
        path,
        ["filename", "first_ping_time", "last_ping_time"],
        ["first_ping_time", "last_ping_time"],
    )

    assert manifest.columns.tolist() == [
        "filename",
        "first_ping_time",
        "last_ping_time",
    ]
    assert manifest.loc[0, "first_ping_time"] == pd.Timestamp("2025-06-19T00:00:00Z")
    assert pd.isna(manifest.loc[0, "last_ping_time"])

    write_manifest(manifest, path)

    assert path.exists()
    assert not Path(f"{path}.tmp").exists()


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
