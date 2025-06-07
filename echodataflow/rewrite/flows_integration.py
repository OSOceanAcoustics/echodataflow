
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

from prefect import task, flow


# Set up paths
data_path = Path("/Users/wujung/code_git/echodataflow/temp_bio")
NASC_path = data_path / "nasc_zarr"


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


@flow(log_prints=True)
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

    if NASC_to_process is None:
        print("No new NASC files to process.")
        return
    else:
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
