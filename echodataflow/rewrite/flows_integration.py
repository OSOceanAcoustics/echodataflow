
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr

from geopy.distance import distance
import geopandas as gpd

import echopype as ep

from core import GRID_PARAMS
from grid import create_boundary_gdf, create_grid_from_bounds
from flows_biology import add_stratum

from prefect import task, flow, get_run_logger


# Turn on verbose logging for echopype
# otherwise all logging will be muted
ep.utils.log.verbose()


# Set up paths
data_main = "/Users/wujung/code_git/echodataflow/temp_bio"
NASC_path = "nasc_zarr"


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



@task()
def task_combine_NASC_to_dataframe(
    path_NASC_files: str,
    NASC_filenames: list
) -> pd.DataFrame:
    """
    Combine multiple NASC datasets into a single DataFrame.
    """
    df_NASC_list = []
    for nascf in NASC_filenames:
        nascf = path_NASC_files / nascf
        ds_NASC = xr.open_zarr(nascf)
        ds_NASC = ep.consolidate.swap_dims_channel_frequency(ds_NASC)
        df_NASC = ds_NASC.sum("depth").sel(frequency_nominal=38000).to_dataframe()
        df_NASC["filename"] = nascf.name
        df_NASC_list.append(df_NASC)

    return pd.concat(df_NASC_list, ignore_index=True)


@task()
def task_griddify_NASC(
    df_stratum: pd.DataFrame,
    df_NASC: pd.DataFrame,
    utm_num: int = utm_num,
    boundary_gdf_utm: gpd.GeoDataFrame = gdf_boundary_utm,
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


# @task(log_prints=True)
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


@flow(log_prints=True)
def flow_ingest_NASC(
    path_main: str = data_main,
    path_NASC_files: str = "nasc_zarr",
    path_NASC_all: str = "NASC_all.csv",
    path_stratum_mean: str = "stratum_mean.csv",
    path_NASC_all_grid: str = "NASC_all_grid.csv",
    num_NASC_reprocess: int = 1,
):
    """
    Ingest NASC data from zarr files and combine into a single DataFrame.
    """
    logger = get_run_logger()

    # Ensure num_NASC_reprocess is at least 1
    if num_NASC_reprocess < 1:
        num_NASC_reprocess = 1
        raise Warning("num_NASC_ignore must be at least 1, setting it to 1.")

    # Assemble full paths
    path_NASC_files = Path(path_main) / path_NASC_files
    path_NASC_all = Path(path_main) / path_NASC_all

    # Get NASC files already processed
    df_NASC_all = (
        pd.read_csv(path_NASC_all, index_col=0)
        if path_NASC_all.exists() else pd.DataFrame()
    )
    NASC_processed = (
        sorted(df_NASC_all["filename"].unique().tolist())
        if not df_NASC_all.empty else []
    )
    logger.info(f"NASC files already processed: {NASC_processed}")

    # Reprocess the last num_NASC_reprocess files
    NASC_to_reprocess = NASC_processed[-num_NASC_reprocess:]
    NASC_processed = NASC_processed[:-num_NASC_reprocess]
    logger.info(f"NASC files to reprocess: {NASC_to_reprocess}")

    # Remove df_NASC_all entries that match files to reprocess
    if not df_NASC_all.empty:
        df_NASC_all = df_NASC_all[~df_NASC_all["filename"].isin(NASC_to_reprocess)]

    # Get all NASC files in the directory
    NASC_all = list(path_NASC_files.glob("*.zarr"))
    NASC_all = sorted([f.name for f in NASC_all])
    logger.info(f"All NASC files: {NASC_all}")

    # Determine which files to process
    NASC_to_process = sorted(list(set(NASC_all).difference(set(NASC_processed))))
    logger.info(f"NASC files to process: {NASC_to_process}")

    if len(NASC_to_process) == 0:
        logger.info("No new NASC files to process.")
        return
    else:
        files_to_process = ""
        for nascf in NASC_to_process:
            files_to_process += f"- {nascf}\n"
        logger.info(f"Files to process:\n{files_to_process}")

        # Combine all unprocessed NASC datasets into a single DataFrame
        df_NASC = task_combine_NASC_to_dataframe(path_NASC_files, NASC_to_process)
        logger.info(f"df_NASC contains: {list(df_NASC["filename"].unique())}")

        # Append new NASC data to existing data
        df_NASC_all = pd.concat([df_NASC_all, df_NASC], ignore_index=True)
        logger.info(f"after run df_NASC_all contains: {list(df_NASC_all["filename"].unique())}")

        # Save to combined NASC data to csv
        df_NASC_all.to_csv(path_NASC_all)

        # Grifdify the NASC data
        df_stratum = pd.read_csv(Path(path_main) / path_stratum_mean, index_col=0)
        gdf_NASC = task_griddify_NASC(
            df_stratum=df_stratum,
            df_NASC=df_NASC_all,
            utm_num=utm_num,
            boundary_gdf_utm=gdf_boundary_utm
        )
        gdf_NASC.to_csv(Path(path_main) / path_NASC_all_grid)