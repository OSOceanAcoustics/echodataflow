import re
from pathlib import Path
import numpy as np
import pandas as pd

from core import TS_L_PARAMS


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


def add_stratum(df, df_stratum):
    # Create latitude bins from the stratum definitions
    lat_bins = [-90.0] + df_stratum["latitude_northern_limit"].tolist() + [90.0]
    lat_labels = df_stratum.index.tolist() + [max(df_stratum.index) + 1]

    # Add stratum column based on td_latitude
    df["stratum"] = pd.cut(
        df["td_latitude"],
        bins=lat_bins,
        labels=lat_labels,
        include_lowest=True
    ).astype(int)

    return df


def get_sigma_bs_mean_stratum(
    df_length_count: pd.DataFrame,
) -> pd.DataFrame:
    # Compute sigma_bs for each row
    df_length_count["sigma_bs"] = 10 ** ((
        TS_L_PARAMS["slope"] * np.log10(df_length_count["rounded_length"])
        + TS_L_PARAMS["intercept"]
    ) / 10)

    # Get mean sigma_bs for each stratum
    return df_length_count.groupby("stratum").apply(
        lambda df: pd.Series(
            (df["sigma_bs"] * df["frequency"]).sum() / df["frequency"].sum(),
            index=["sigma_bs_mean"],
        ),
        include_groups=False
    ).reset_index()


def get_weight_mean_stratum(
    df_length_count: pd.DataFrame,
    df_length_weight_regression: pd.DataFrame,
) -> pd.DataFrame:
    # Merge length-weight regression coefficients
    df_merged = pd.merge(
        df_length_count,
        df_length_weight_regression[df_length_weight_regression["sex"] == "all"][["stratum", "p1", "p2"]],
        on="stratum",
        how="left"
    )

    # Then compute weight using the length-weight relationship: W = 10^(p1 * log10(L) + p2)
    df_merged["weight"] = 10 ** (
        df_merged["p1"] * np.log10(df_merged["rounded_length"]) + df_merged["p2"]
    )

    # Get mean weight for each stratum
    return df_merged.groupby("stratum").apply(
        lambda df: pd.Series(
            (df["weight"] * df["frequency"]).sum() / df["frequency"].sum(),
            index=["weight_mean"],
        ),
        include_groups=False
    ).reset_index()



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
        haul_num = 11
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
        df_specimen = pd.merge(
            df_specimen,
            df_info,
            how="cross"  # create all possible combinations, but we only have 1 row here
        )
        df_length_count = pd.merge(
            df_length_count,
            df_info, 
            how="cross"  # create all possible combinations, but we only have 1 row here
        )

        # Add stratrum info to df_specimen and df_length_count based on latitude
        df_stratum = pd.read_csv(data_path / "inpfc_def.csv", index_col=0)
        df_specimen = add_stratum(df_specimen, df_stratum)
        df_length_count = add_stratum(df_length_count, df_stratum)

        # Compute length-weight relationship for each stratum
        # Separately for: male, female, all fish combined
        df_length_weight_regression = get_length_weight_regression(df_specimen)

        # Compute mean sigma_bs and mean weight for each stratum
        # columns: stratum, sigma_bs_mean, weight_mean
        df_stratum = get_sigma_bs_mean_stratum(df_length_count)
        df_weight_mean = get_weight_mean_stratum(df_length_count, df_length_weight_regression)
        df_stratum = df_stratum.merge(df_weight_mean, on="stratum", how="outer")
        