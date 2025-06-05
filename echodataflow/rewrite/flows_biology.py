import re
from pathlib import Path
import numpy as np
import pandas as pd


data_path = Path("/Users/wujung/code_git/echodataflow/temp_bio")
csv_path = data_path / "bio_csv"

# Initialize haul sigma_bs dataframe
df_haul_sigma_bs = pd.DataFrame(
    columns=[
        "haul_num",
        "species_code",
        "latitude",
        "longitude",
        "length_bin",
        "length_count",
    ]
)
df_haul_sigma_bs.to_csv(csv_path / "haul_sigma_bs.csv")


def get_valid_hauls(
    date_prefix: str,
    species_code: int,
    bio_filenames: dict,
):
    # Pull out haul numbers (operation_number in csv)
    HAUL_NUM_PATTERN = rf"{date_prefix}_({species_code}_)?(?P<haul_num>\d{{3}})_\w+\.csv"
    haul_num_all = {k: set() for k in bio_filenames.keys()}
    for file_type in bio_filenames.keys():
        for fname in bio_filenames[file_type]:
            haul_num_all[file_type].add(int(re.match(HAUL_NUM_PATTERN, fname.name)["haul_num"]))

    # Each haul number should have 4 files as defined above
    valid_hauls = set.union(*haul_num_all.values())
    haul_num_to_remove = set()
    for file_type in bio_filenames.keys():
        haul_num_diff = valid_hauls.difference(haul_num_all[file_type])
        if haul_num_diff:
            haul_num_to_remove.update(haul_num_diff)
    valid_hauls.difference_update(haul_num_to_remove)
    return valid_hauls


def get_count_from_length_specimen(
    df_length: pd.DataFrame,
    df_specimen: pd.DataFrame,
) -> pd.DataFrame:
    """
    Get length count from length and specimen dataframes.
    """
    # Round fish length to nearest integer
    df_specimen["rounded_length"] = df_specimen["length"].round(0)

    # Get length count from both dataframes
    specimen_counts = df_specimen.groupby(["sex", "rounded_length"]).size().reset_index(name="frequency")
    length_counts = df_length.groupby(["sex", "rounded_length"]).agg({"frequency": "sum"}).reset_index()
    
    df_combined = pd.merge(
        specimen_counts.reset_index(),
        length_counts.reset_index(),
        on=["sex", "rounded_length"],
        how="outer"
    ).fillna(0)
    df_combined["frequency"] = (df_combined["frequency_x"] + df_combined["frequency_y"]).astype(int)

    return df_combined[["sex", "rounded_length", "frequency"]]


def get_length_weight_regression(df_specimen: pd.DataFrame) -> pd.DataFrame:
    """
    Get length-weight regression coefficients for male, female, and all fish combined.
    """
    # Male, female separately
    df_regres = df_specimen.groupby(["sex", "stratum"]).apply(
        lambda df: pd.Series(
            np.polyfit(np.log10(df["length"]), np.log10(df["organism_weight"]), 1),
            index=["p1", "p2"],
        ),
        include_groups = False
    ).reset_index()

    # All fish combined
    df_all = df_specimen.groupby("stratum").apply(
        lambda df: pd.Series(
            np.polyfit(np.log10(df["length"]),
                    np.log10(df["organism_weight"]), 1),
            index=["p1", "p2"],
        ),
        include_groups = False
    ).reset_index()
    df_all["sex"] = "all"

    # Combine all and make everything lowercase
    df_regres = pd.concat([df_regres, df_all]).reset_index()
    df_regres["sex"] = df_regres["sex"].str.lower()
    
    return df_regres


def ingest_biological_data(
    bio_path: str = data_path,
    date_prefix: str = "202407",
    species_code: int = 22500,
):

    # Get all filenames
    bio_filenames = {
        "length": list(bio_path.glob(f"{date_prefix}_{species_code}_*_lf.csv")),
        "specimen": list(bio_path.glob(f"{date_prefix}_{species_code}_*_spec.csv")),
        "catch": list(bio_path.glob(f"{date_prefix}_*_catch_perc.csv")),
        "info": list(bio_path.glob(f"{date_prefix}_*_operation_info.csv")),
    }

    # Get valid haul numbers (those with 4 files)
    hauls_valid = get_valid_hauls(date_prefix, species_code, bio_filenames)

    # Get hauls to process
    df_haul_sigma_bs = pd.read_csv(df_haul_sigma_bs, index_col=0)
    hauls_processed = set(df_haul_sigma_bs["haul_num"].unique())
    hauls_to_process = hauls_valid.difference(hauls_processed)
    
    if not hauls_to_process:  # if there are hauls to process
        print(f"No hauls to process for {date_prefix} with species code {species_code}.")
        return
    else:
        for haul_num in hauls_to_process:
            print(f"Processing haul number {haul_num} for {date_prefix} with species code {species_code}.")

            # Consolidate length count for each haul
            df_length = pd.read_csv(
                csv_path / f"{date_prefix}_{species_code}_{haul_num:03d}_lf.csv", index_col=0
            )
            df_specimen = pd.read_csv(
                csv_path / f"{date_prefix}_{species_code}_{haul_num:03d}_spec.csv", index_col=0
            )
            df_length_count = get_count_from_length_specimen(df_length, df_specimen)

            # Add haul number and lat/lon for downstream stratification
            df_info = pd.read_csv(
                csv_path / f"{date_prefix}_{haul_num:03d}_operation_info.csv", index_col=0
            ).reset_index()  # reset index to get operation_number into a column
            df_length_count = pd.merge(
                df_length_count,
                df_info, 
                how="cross"  # create all possible combinations, but we only have 1 row here
            )

            # Add latitude and longitude to specimen data
            df_specimen_with_lat_lon = pd.merge(
                df_specimen,
                df_info[["td_latitude", "td_longitude"]],
                how="cross"  # create all possible combinations, but we only have 1 row here
            )

            # Compute length-weight relationship: male, female, and all fish combined
            df_length_weight_regression = get_length_weight_regression(df_specimen)
