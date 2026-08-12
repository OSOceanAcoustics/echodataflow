from pathlib import Path

import pandas as pd
from prefect import flow


@flow(log_prints=True)
def flow_transect_update(
    path_transect_csv: str,
    path_snapshot_csv: str,
    path_main: str,
    file_Sv_csv: str = "Sv_files.csv",
):
    """Identify updated transects and find overlapping Sv files."""

    path_transect = Path(path_transect_csv)
    path_snapshot = Path(path_snapshot_csv)
    path_sv_csv = Path(path_main) / file_Sv_csv

    # Read the current transect information, preserving transect identifiers
    # as strings so values with leading zeros (e.g., "002") are not converted
    # to integers by pandas.
    current = pd.read_csv(
        path_transect,
        dtype={
            "transectPart": str,
            "transectNumber": str,
        },
    )

    if not path_snapshot.exists():
        print("No previous transect snapshot found. Initializing snapshot.")
        current.to_csv(path_snapshot, index=False)
        return

    previous = pd.read_csv(
        path_snapshot,
        dtype={
            "transectPart": str,
            "transectNumber": str,
        },
    )

    key_columns = [
        "transectPart",
        "transectNumber",
        "transectStart",
        "transectEnd",
    ]

    changed = (
        current.merge(
            previous[key_columns],
            on=key_columns,
            how="left",
            indicator=True,
        )
        .query("_merge == 'left_only'")
        .drop(columns="_merge")
        .drop_duplicates(subset=key_columns)
    )

    if changed.empty:
        print("No new or updated transect segments.")
        current.to_csv(path_snapshot, index=False)
        return

    print(f"Found {len(changed)} new or updated transect segment(s):")
    print(changed)

    # Load Sv tracking information created by raw2Sv
    if not path_sv_csv.exists():
        print(f"Sv tracking file does not exist yet: {path_sv_csv}")
        return

    df_sv = pd.read_csv(
        path_sv_csv,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"],
    )

    if df_sv["first_ping_time"].dt.tz is None:
        df_sv["first_ping_time"] = df_sv["first_ping_time"].dt.tz_localize("UTC")

    if df_sv["last_ping_time"].dt.tz is None:
        df_sv["last_ping_time"] = df_sv["last_ping_time"].dt.tz_localize("UTC")

    # Find Sv files overlapping each changed transect
    for _, transect in changed.iterrows():
        start_time = pd.to_datetime(transect["transectStart"], utc=True)
        end_time = pd.to_datetime(transect["transectEnd"], utc=True)

        sv_filenames = sorted(
            df_sv[
                (df_sv["last_ping_time"] >= start_time)
                & (df_sv["first_ping_time"] <= end_time)
            ]["Sv_filename"].tolist()
        )

        print(
            f"\nTransect {transect['transectPart']}: "
            f"{start_time} to {end_time}"
        )
        print(f"Found {len(sv_filenames)} overlapping Sv file(s):")

        for filename in sv_filenames:
            print(f"- {filename}")

    # Save snapshot only after processing the current CSV
    current.to_csv(path_snapshot, index=False)