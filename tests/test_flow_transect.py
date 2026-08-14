import pandas as pd

from echodataflow.flows.flows_transect import (
    find_overlapping_sv_files,
    flow_transect_update,
    get_changed_transects,
)


def test_flow_transect_update_initializes_snapshot(tmp_path):
    transect_csv = tmp_path / "transects.csv"
    snapshot_csv = tmp_path / "snapshot.csv"
    path_main = tmp_path / "output"
    path_main.mkdir()

    pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:00:00Z"],
            "transectEnd": ["2024-07-07T00:10:00Z"],
        }
    ).to_csv(transect_csv, index=False)

    flow_transect_update.fn(
        path_transect_csv=str(transect_csv),
        path_snapshot_csv=str(snapshot_csv),
        path_main=str(path_main),
    )

    assert snapshot_csv.exists()

    snapshot = pd.read_csv(
        snapshot_csv,
        dtype={
            "transectPart": str,
            "transectNumber": str,
        },
    )

    assert len(snapshot) == 1
    assert snapshot.loc[0, "transectPart"] == "001"


def test_flow_transect_update_finds_overlapping_sv(tmp_path, capsys):
    transect_csv = tmp_path / "transects.csv"
    snapshot_csv = tmp_path / "snapshot.csv"
    path_main = tmp_path / "output"
    path_main.mkdir()

    previous = pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:00:00Z"],
            "transectEnd": ["2024-07-07T00:10:00Z"],
        }
    )

    current = pd.concat(
        [
            previous,
            pd.DataFrame(
                {
                    "transectPart": ["002"],
                    "transectNumber": ["002"],
                    "transectStart": ["2024-07-07T00:20:00Z"],
                    "transectEnd": ["2024-07-07T00:30:00Z"],
                }
            ),
        ],
        ignore_index=True,
    )

    previous.to_csv(snapshot_csv, index=False)
    current.to_csv(transect_csv, index=False)

    pd.DataFrame(
        {
            "Sv_filename": [
                "before_Sv.zarr",
                "overlap_Sv.zarr",
                "after_Sv.zarr",
            ],
            "first_ping_time": [
                "2024-07-07T00:00:00Z",
                "2024-07-07T00:15:00Z",
                "2024-07-07T00:40:00Z",
            ],
            "last_ping_time": [
                "2024-07-07T00:05:00Z",
                "2024-07-07T00:25:00Z",
                "2024-07-07T00:50:00Z",
            ],
        }
    ).to_csv(path_main / "Sv_files.csv")

    flow_transect_update.fn(
        path_transect_csv=str(transect_csv),
        path_snapshot_csv=str(snapshot_csv),
        path_main=str(path_main),
    )

    output = capsys.readouterr().out

    assert "Found 1 new or updated transect segment(s)" in output
    assert "Transect 002" in output
    assert "Found 1 overlapping Sv file(s)" in output
    assert "overlap_Sv.zarr" in output


def test_get_changed_transects():
    previous = pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:00:00Z"],
            "transectEnd": ["2024-07-07T00:10:00Z"],
        }
    )

    current = pd.concat(
        [
            previous,
            pd.DataFrame(
                {
                    "transectPart": ["002"],
                    "transectNumber": ["002"],
                    "transectStart": ["2024-07-07T00:20:00Z"],
                    "transectEnd": ["2024-07-07T00:30:00Z"],
                }
            ),
        ],
        ignore_index=True,
    )

    changed = get_changed_transects(current, previous)

    assert len(changed) == 1
    assert changed.iloc[0]["transectPart"] == "002"


def test_find_overlapping_sv_files():
    df_sv = pd.DataFrame(
        {
            "Sv_filename": [
                "before_Sv.zarr",
                "overlap_Sv.zarr",
                "after_Sv.zarr",
            ],
            "first_ping_time": pd.to_datetime(
                [
                    "2024-07-07T00:00:00Z",
                    "2024-07-07T00:15:00Z",
                    "2024-07-07T00:40:00Z",
                ],
                utc=True,
            ),
            "last_ping_time": pd.to_datetime(
                [
                    "2024-07-07T00:05:00Z",
                    "2024-07-07T00:25:00Z",
                    "2024-07-07T00:50:00Z",
                ],
                utc=True,
            ),
        }
    )

    overlapping = find_overlapping_sv_files(
        df_sv,
        pd.Timestamp("2024-07-07T00:20:00Z"),
        pd.Timestamp("2024-07-07T00:30:00Z"),
    )

    assert overlapping["Sv_filename"].tolist() == ["overlap_Sv.zarr"]
    
def test_flow_transect_update_ignores_open_transect(tmp_path, capsys):
    transect_csv = tmp_path / "transects.csv"
    snapshot_csv = tmp_path / "snapshot.csv"
    path_main = tmp_path / "output"
    path_main.mkdir()

    previous = pd.DataFrame(
        {
            "transectPart": ["001"],
            "transectNumber": ["001"],
            "transectStart": ["2024-07-07T00:00:00Z"],
            "transectEnd": ["2024-07-07T00:10:00Z"],
        }
    )

    current = pd.concat(
        [
            previous,
            pd.DataFrame(
                {
                    "transectPart": ["002"],
                    "transectNumber": ["002"],
                    "transectStart": ["2024-07-07T00:20:00Z"],
                    "transectEnd": [pd.NA],
                }
            ),
        ],
        ignore_index=True,
    )

    previous.to_csv(snapshot_csv, index=False)
    current.to_csv(transect_csv, index=False)

    flow_transect_update.fn(
        path_transect_csv=str(transect_csv),
        path_snapshot_csv=str(snapshot_csv),
        path_main=str(path_main),
    )

    output = capsys.readouterr().out

    assert "No new or updated transect segments." in output