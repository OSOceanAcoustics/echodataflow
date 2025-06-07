import re
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr

from core import GRID_PARAMS
from geopy.distance import distance
import geopandas as gpd
from shapely.geometry import box

import echopype as ep

from core import TS_L_PARAMS, INFO_DATAFRAME_MAPPING
from grid import create_boundary_gdf, create_grid_from_bounds

from prefect import task, flow


data_path = Path("/Users/wujung/code_git/echodataflow/temp_bio")
csv_path = data_path / "bio_csv"
NASC_path = data_path / "nasc_zarr"

# Initialize dataframes
df_haul_info_all = pd.DataFrame(
    columns=["operation_number", "timestamp", "latitude", "longitude"]
)
df_specimen_all = pd.DataFrame(
    columns=["operation_number", "partition", "species", "sex", "rounded_length", "frequency"]
)
df_length_all = pd.DataFrame(
    columns=[
        "operation_number", "partition", "species",
        "length_type", "length", "sex", "organism_weight", "barcode"
    ]
)
df_length_count_all = pd.DataFrame(
    columns=["sex", "rounded_length", "frequency", "operation_number"]
)
df_haul_info_all.to_csv(data_path / "haul_info_all.csv")
df_specimen_all.to_csv(data_path / "specimen_all.csv")
df_length_all.to_csv(data_path / "length_all.csv")
df_length_count_all.to_csv(data_path / "length_count_all.csv")


# Path to dataframe containing processed NASC filenames
NASC_processed_path = data_path / "nasc_processed.csv"
if not NASC_processed_path.exists():
    # Create an empty dataframe to store processed NASC filenames
    df_NASC_processed = pd.DataFrame(columns="filename")
    df_NASC_processed.to_csv(NASC_processed_path, index=False)


# Create boundary GeoDataFrame with UTM projection
gdf_boundary, gdf_boundary_utm, utm_num = create_boundary_gdf(
    bounds=GRID_PARAMS["bounds"], 
    projection=GRID_PARAMS["projection"]
)

# Create the full grid
gdf_grid_cells, gdf_coastline, _ = create_grid_from_bounds(
    bounds=GRID_PARAMS["bounds"], 
    resolution=GRID_PARAMS["resolution"],
    projection=GRID_PARAMS["projection"],
    coastline_resolution="10m",
    area_threshold=5
)
gdf_grid_cells.set_index(["grid_x", "grid_y"], inplace=True)




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
    specimen_counts = df_specimen.groupby(["sex", "rounded_length", "operation_number"]).size().reset_index(name="frequency")
    length_counts = df_length.groupby(["sex", "rounded_length", "operation_number"]).agg({"frequency": "sum"}).reset_index()
    
    df_combined = pd.merge(
        specimen_counts.reset_index(),
        length_counts.reset_index(),
        on=["sex", "rounded_length", "operation_number"],
        how="outer"
    ).fillna(0)
    df_combined["frequency"] = (df_combined["frequency_x"] + df_combined["frequency_y"]).astype(int)

    return df_combined[["sex", "rounded_length", "frequency", "operation_number"]]


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
    lat_labels = df_stratum["stratum"].tolist() + [max(df_stratum["stratum"]) + 1]

    # Add stratum column based on latitude
    df["stratum"] = pd.cut(
        df["latitude"],
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


@flow(log_prints=True)
def ingest_biological_data(
    bio_path: str = csv_path,
    date_prefix: str = "202407",
    species_code: int = 22500,
    haul_info_all_path: str = data_path / "haul_info_all.csv",
    specimen_all_path: str = data_path / "specimen_all.csv",
    length_all_path: str = data_path / "length_all.csv",
    length_count_all_path: str = data_path / "length_count_all.csv",
    stratum_mean_path: str = data_path / "stratum_mean.csv",
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
    df_haul_info_all = pd.read_csv(haul_info_all_path, index_col=0)
    if df_haul_info_all.empty:
        hauls_processed = set()
    else:
        hauls_processed = set(df_haul_info_all["operation_number"].unique())
    hauls_to_process = list(hauls_valid.difference(hauls_processed))
    
    if not hauls_to_process:  # if there are hauls to process
        print(f"No hauls to process for {date_prefix} with species code {species_code}.")
        return
    else:
        print(
            f"Processing {len(hauls_to_process)} hauls for "
            f"{date_prefix} with species code {species_code}:\n",
            hauls_to_process
        )
        # Load dataframes from all hauls to process
        df_length = []
        df_specimen = []
        df_info = []
        for haul_num in hauls_to_process:
            df_length.append(
                pd.read_csv(
                    csv_path / f"{date_prefix}_{species_code}_{haul_num:03d}_lf.csv", index_col=0
                ).reset_index()
            )
            df_specimen.append(
                pd.read_csv(
                    csv_path / f"{date_prefix}_{species_code}_{haul_num:03d}_spec.csv", index_col=0
                ).reset_index()
            )
            df_info.append(
                pd.read_csv(
                    csv_path / f"{date_prefix}_{haul_num:03d}_operation_info.csv", index_col=0
                ).rename(columns=INFO_DATAFRAME_MAPPING).reset_index()  # reset index to get operation_number into a column
            )
        df_length = pd.concat(df_length, ignore_index=True)
        df_specimen = pd.concat(df_specimen, ignore_index=True)
        df_info = pd.concat(df_info, ignore_index=True)

        # Combined length frequency from length and specimen dataframes
        df_length_count = get_count_from_length_specimen(df_length, df_specimen)

        # Add haul number and lat/lon for downstream stratification
        df_specimen = pd.merge(
            df_specimen,
            df_info,
            on="operation_number",
            how="left"
        )
        df_length_count = pd.merge(
            df_length_count,
            df_info,
            on="operation_number",
            how="left"
        )

        # Update df_haul_info_all, df_specimen_all, df_length_all
        df_specimen_all = pd.read_csv(specimen_all_path, index_col=0)
        df_length_all = pd.read_csv(length_all_path, index_col=0)
        df_length_count_all = pd.read_csv(length_count_all_path, index_col=0)
        df_haul_info_all = pd.read_csv(haul_info_all_path, index_col=0)

        df_specimen_all = pd.concat([df_specimen_all, df_specimen], ignore_index=True)
        df_length_all = pd.concat([df_length_all, df_length], ignore_index=True)
        df_length_count_all = pd.concat([df_length_count_all, df_length_count], ignore_index=True)
        df_haul_info_all = pd.concat([df_haul_info_all, df_info], ignore_index=True)

        df_specimen_all.to_csv(specimen_all_path)
        df_length_all.to_csv(length_all_path)
        df_length_count_all.to_csv(length_count_all_path)
        df_haul_info_all.to_csv(haul_info_all_path)

        # Add stratrum info to df_specimen and df_length_count based on latitude
        df_stratum = pd.read_csv(data_path / "inpfc_def.csv", index_col=0).reset_index().rename(
            columns={"stratum_num": "stratum"}
        )
        df_specimen_all = add_stratum(df_specimen_all, df_stratum)
        df_length_count_all = add_stratum(df_length_count_all, df_stratum)

        # Compute length-weight relationship for each stratum
        # Separately for: male, female, all fish combined
        df_length_weight_regression = get_length_weight_regression(df_specimen_all)

        # Compute mean sigma_bs and mean weight for each stratum
        # columns: stratum, sigma_bs_mean, weight_mean
        df_stratum = pd.merge(
            df_stratum,
            get_sigma_bs_mean_stratum(df_length_count_all),
            on="stratum",
            how="outer"
        )
        df_stratum = pd.merge(
            df_stratum,
            get_weight_mean_stratum(df_length_count_all, df_length_weight_regression),
            on="stratum",
            how="outer"
        )
        df_stratum.to_csv(stratum_mean_path)


def combine_NASC_dataset_to_dataframe(
    NASC_path: Path,
    NASC_filenames: list
) -> pd.DataFrame:
    """
    Combine multiple NASC datasets into a single DataFrame.
    """
    df_NASC_list = []
    for nascf in NASC_filenames:
        ds_NASC = xr.open_zarr(NASC_path / nascf)
        ds_NASC = ep.consolidate.swap_dims_channel_frequency(ds_NASC)
        df_NASC = ds_NASC.sum("depth").sel(frequency_nominal=38000).to_dataframe()
        df_NASC["filename"] = nascf
        df_NASC_list.append(df_NASC)

    return pd.concat(df_NASC_list, ignore_index=True)


@task(log_prints=True)
def griddify_NASC(
    df_NASC: pd.DataFrame,
    utm_num: int,
    boundary_gdf_utm: gpd.GeoDataFrame,
    df_stratum: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """
    Generate geodataframe with grid x/y containing NASC values.
    """
    # Convert to GeoDataFrame
    gdf_NASC = gpd.GeoDataFrame(
        data=df_NASC,
        geometry=gpd.points_from_xy(df_NASC["longitude"], df_NASC["latitude"]),
        crs=GRID_PARAMS["projection"],
    ).to_crs(f"epsg:{utm_num}")

    # Extract x y coordinate for pd.cut below
    gdf_NASC["utm_x"] = gdf_NASC["geometry"].x
    gdf_NASC["utm_y"] = gdf_NASC["geometry"].y

    # Get grid step sizes and boundary x/y
    x_step = distance(nautical=GRID_PARAMS["resolution"]["x_distance"]).meters
    y_step = distance(nautical=GRID_PARAMS["resolution"]["y_distance"]).meters
    xmin, ymin, xmax, ymax = boundary_gdf_utm.total_bounds

    # Bin longitude and latitude into grids
    gdf_NASC["grid_x"] = pd.cut(
        gdf_NASC["utm_x"],
        np.arange(xmin, xmax + x_step, x_step),
        right=False,
        labels=np.arange(1, len(np.arange(xmin, xmax + x_step, x_step))),
    ).astype(int)

    # Bin the latitude data
    gdf_NASC["grid_y"] = pd.cut(
        gdf_NASC["utm_y"],
        np.arange(ymin, ymax + y_step, y_step),
        right=True,
        labels=np.arange(1, len(np.arange(ymin, ymax + y_step, y_step))),
    ).astype(int)

    # Add stratum info
    gdf_NASC = add_stratum(gdf_NASC, df_stratum)

    return gdf_NASC


@task(log_prints=True)
def update_grid(
    gdf_grid_cells: gpd.GeoDataFrame,
    gdf_NASC: gpd.GeoDataFrame,
    df_stratum: pd.DataFrame,
):
    """
    Update the grid with the latest NASC data.
    """
    # Merge on stratum 
    gdf_NASC = gdf_NASC.merge(
        df_stratum[["stratum", "sigma_bs_mean", "weight_mean"]],
        on="stratum",  # merge on stratum 
        how="left"
    )

    # Compute stratified number density from NASC
    gdf_NASC["number_density"] = gdf_NASC["NASC"] / gdf_NASC["sigma_bs_mean"]

    # Compute biomass density from number density
    gdf_NASC["biomass_density"] = gdf_NASC["number_density"] * gdf_NASC["weight_mean"]

    # Merge with grid cells
    gdf_grid_cells = pd.merge(
        gdf_grid_cells,
        gdf_NASC.groupby(["grid_x", "grid_y"])["number_density"].mean(),
        on=["grid_x", "grid_y"],
        how="left"
    )
    gdf_grid_cells = pd.merge(
        gdf_grid_cells,
        gdf_NASC.groupby(["grid_x", "grid_y"])["biomass_density"].mean(),
        on=["grid_x", "grid_y"],
        how="left"
    )

    # Compute abundance and biomass
    gdf_grid_cells["abundance"] = gdf_grid_cells["number_density"] * gdf_grid_cells["area"]
    gdf_grid_cells["biomass"] = gdf_grid_cells["biomass_density"] * gdf_grid_cells["area"]

    # Save to csv


@task(log_prints=True)
def ingest_NASC_data(
    NASC_path: str = NASC_path,
    NASC_processed_path: str = NASC_processed_path,
    NASC_all_path: str = data_path / "NASC_all.csv",
    num_NASC_ignore: int = 3,
):
    """
    Ingest NASC data from zarr files and combine into a single DataFrame.
    """
    # Get NASC filenames to process
    NASC_all = list(NASC_path.glob("*.zarr"))
    NASC_all = sorted([f.name for f in NASC_all])
    NASC_ignore = NASC_all[-num_NASC_ignore:]  # NASC files to reprocess
    NASC_all = set(NASC_all[:-num_NASC_ignore])
    NASC_processed = set(
        pd.read_csv(NASC_processed_path, index_col=0).reset_index()["filename"]
    )
    NASC_to_process = list(NASC_all.difference(NASC_processed))

    # Combine all unprocessed NASC datasets into a single DataFrame
    df_NASC = combine_NASC_dataset_to_dataframe(NASC_path, NASC_to_process)

    # Load exising NASC data and remove entries from files that will be reprocessed
    if not NASC_all_path.exists():
        df_NASC_all = pd.DataFrame()
    else:
        df_NASC_all = pd.read_csv(NASC_all_path, index_col=0)
        df_NASC_all = df_NASC_all[~df_NASC_all["filename"].isin(NASC_ignore)]
    
    # Append new NASC data to existing data
    df_NASC_all = pd.concat([df_NASC_all, df_NASC], ignore_index=True)

    # Save to CSV
    df_NASC.to_csv(NASC_all_path, index=False)
