from pathlib import Path
import datetime
import configparser

import pandas as pd
import numpy as np
import xarray as xr
import s3fs

from prefect import flow, get_run_logger

from echodataflow.utils.utils import round_up_mins, get_slice_start_end_times


@flow()
def flow_update_cache_MVBS(
    time_offset_seconds: float = 0.0,
    slice_mins: int = 180,
    path_cache: str = "PATH_TO_DATA_CACHE",
    path_MVBS: str = "PATH_TO_MVBS_DATA_STORE",
    cred_file: str = "PATH_TO_CREDENTIALS_FILE",
    file_MVBS_csv: str = "MVBS_files.csv",
    file_MVBS_zarr: str = "latest_MVBS.zarr",
):
    logger = get_run_logger()

    # Set end_time to current time - time_offset_seconds
    end_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=time_offset_seconds)

    logger.info(
        "flow started with parameters:\n"
        f"- end_time: {end_time}\n"
        f"- slice_mins: {slice_mins}\n"
    )

    # Compute slice time range
    start_time, end_time = get_slice_start_end_times(
        end_time=end_time, slice_mins=slice_mins, num_slices=1
    )

    # Get cloud bucket
    config = configparser.ConfigParser()
    config.read(cred_file)
    fs = s3fs.S3FileSystem(
        key=config["osn_sdsc_hake"]["access_key_id"],
        secret=config["osn_sdsc_hake"]["secret_access_key"],
        client_kwargs={"endpoint_url": config["osn_sdsc_hake"]["endpoint"]},
    )

    # Get all MVBS files in the bucket
    MVBS_all = fs.glob(f"{path_MVBS}/*.zarr")
    MVBS_all = sorted([Path(f).name for f in MVBS_all])
    # logger.info(f"All MVBS files: {MVBS_all}")

    # Load MVBS info dataframe
    with fs.open(str(Path(path_MVBS).parent / file_MVBS_csv), "r") as f:
        df_MVBS = pd.read_csv(
            f,
            parse_dates=["first_ping_time", "last_ping_time"],
            index_col=0,
        )

    # Convert last_ping_time and first_ping_time to UTC
    if not df_MVBS.empty:
        if df_MVBS["last_ping_time"].dt.tz is None:
            df_MVBS["last_ping_time"] = df_MVBS["last_ping_time"].dt.tz_localize("UTC")
        if df_MVBS["first_ping_time"].dt.tz is None:
            df_MVBS["first_ping_time"] = df_MVBS["first_ping_time"].dt.tz_localize("UTC")

    # Get MVBS files in the specified time range (only 1 slice)
    MVBS_filenames = sorted(
        df_MVBS[
            (pd.to_datetime(df_MVBS["last_ping_time"]) >= start_time[0]) &
            (pd.to_datetime(df_MVBS["first_ping_time"]) <= end_time[0])
        ]["MVBS_filename"].tolist()
    )
    logger.info(
        f"Found {len(MVBS_filenames)} MVBS files in the specified time range: \n"
        + "".join([f"- {mvbsf}\n" for mvbsf in MVBS_filenames])
    )

    if len(MVBS_filenames) == 0:
        logger.info("MVBS cache not updated: no MVBS files in the specified time range")
        return
    else:
        # Assmeble fs mapper for the MVBS files
        MVBS_filenames = [
            fs.get_mapper(str(Path(path_MVBS) / mvbsf)) for mvbsf in MVBS_filenames
        ]

        # Combine and prepare MVBS dataset
        ds_MVBS = xr.open_mfdataset(
            MVBS_filenames,
            parallel=True,
            coords="minimal",
            data_vars="minimal",
            compat='override',
            chunks={"channel": -1, "ping_time": -1, "depth": -1},  # load everything into 1 chunk
            engine="zarr",  # use zarr engine for reading
            consolidated=False,
        )
        # TODO: echo_range:depth swap can be removed once Echoshader is fixed
        ds_MVBS["echo_range"] = ds_MVBS["depth"]
        ds_MVBS = ds_MVBS.swap_dims({"depth": "echo_range"})

        # Add actual_range to allow using holoviz
        ds_MVBS["Sv"] = ds_MVBS["Sv"].assign_attrs(
            actual_range=(float(ds_MVBS["Sv"].min().compute()),
                        float(ds_MVBS["Sv"].max().compute()))
        )

        # Remove chunk encoding to prevent saving issues
        for var in ds_MVBS.data_vars:
            if "chunks" in ds_MVBS[var].encoding:
                ds_MVBS[var].encoding.pop("chunks")
            if "preferred_chunks" in ds_MVBS[var].encoding:
                ds_MVBS[var].encoding.pop("preferred_chunks")

        # Save to cache
        logger.info(f"Saving MVBS dataset to cache: {str(Path(path_cache) / file_MVBS_zarr)}")
        ds_MVBS.chunk(
            {"channel": -1, "ping_time": -1, "echo_range": -1}
        ).to_zarr(
            Path(path_cache) / file_MVBS_zarr,  # cache is local
            mode="w",
            consolidated=True,
        )

def _prepare_sv_for_echogram(
    ds: xr.Dataset,
    var_name: str = "Sv_masked",
) -> xr.Dataset:
    """Prepare an Sv dataset for Echoshader visualization."""

    plot_ds = xr.Dataset(
        {
            "Sv": ds["Sv"],
            "Sv_masked": ds[var_name],
        }
    )

    if "frequency_nominal" in ds:
        plot_ds["frequency_nominal"] = ds["frequency_nominal"]

    if "depth" in ds:
        vertical = ds["depth"]
    elif "echo_range" in ds:
        vertical = ds["echo_range"]
    else:
        vertical = ds["range_sample"]

    reduce_dims = [
        dim
        for dim in ["channel", "ping_time"]
        if dim in vertical.dims
    ]

    if reduce_dims:
        vertical = vertical.median(
            dim=reduce_dims,
            skipna=True,
        )

    vertical_values = np.asarray(vertical.values)

    # Sv datasets may have trailing range samples with no valid depth.
    # Keep the contiguous valid part used for visualization.
    finite = np.isfinite(vertical_values)

    if finite.any():
        invalid = np.flatnonzero(~finite)

        if invalid.size:
            valid_length = int(invalid[0])
        else:
            valid_length = vertical_values.size

        plot_ds = plot_ds.isel(
            range_sample=slice(0, valid_length)
        )

        vertical_values = vertical_values[:valid_length]

    if (
        vertical_values.size == 0
        or not np.isfinite(vertical_values).all()
    ):
        raise ValueError(
            "Could not construct a finite vertical coordinate "
            "for Sv visualization."
        )

    plot_ds = plot_ds.assign_coords(
        echo_range=(
            "range_sample",
            vertical_values,
        )
    )

    plot_ds = plot_ds.swap_dims(
        {"range_sample": "echo_range"}
    )

    vmin = float(
        plot_ds["Sv"].min(skipna=True).compute()
    )
    vmax = float(
        plot_ds["Sv"].max(skipna=True).compute()
    )

    plot_ds["Sv"] = plot_ds["Sv"].assign_attrs(
        actual_range=(vmin, vmax)
    )

    masked_vmin = float(
        plot_ds["Sv_masked"].min(skipna=True).compute()
    )
    masked_vmax = float(
        plot_ds["Sv_masked"].max(skipna=True).compute()
    )

    plot_ds["Sv_masked"] = plot_ds["Sv_masked"].assign_attrs(
        actual_range=(masked_vmin, masked_vmax)
    )

    for var in plot_ds.variables:
        plot_ds[var].encoding.pop("chunks", None)
        plot_ds[var].encoding.pop(
            "preferred_chunks",
            None,
        )

    return plot_ds

@flow()
def flow_update_cache_CPS(
    path_CPS: str,
    path_cache: str,
    path_transect_csv: str,
    file_CPS_zarr: str = "latest_CPS.zarr",
):
    """Update visualization cache from the latest CPS transect product."""

    path_CPS = Path(path_CPS)
    path_cache = Path(path_cache)
    path_transect_csv = Path(path_transect_csv)

    path_cache.mkdir(
        parents=True,
        exist_ok=True,
    )

    # -----------------------------------------------------
    # Find latest completed CPS transect
    # -----------------------------------------------------

    cps_files = sorted(
        path_CPS.glob("transect_*_CPS.zarr"),
        key=lambda path: path.stat().st_mtime,
    )

    if not cps_files:
        print(
            f"CPS cache not updated: "
            f"no CPS transects found in {path_CPS}"
        )
        return

    latest_cps = cps_files[-1]

    transect_number = (
        latest_cps.name
        .replace("transect_", "")
        .replace("_CPS.zarr", "")
    )

    # -----------------------------------------------------
    # Read transect metadata
    # -----------------------------------------------------

    df_transect = pd.read_csv(
        path_transect_csv,
        dtype={
            "transectPart": str,
            "transectNumber": str,
        },
    )

    transect_rows = df_transect[
        df_transect["transectPart"]
        == transect_number
    ]

    if transect_rows.empty:
        print(
            f"Transect {transect_number} "
            f"not found in {path_transect_csv}"
        )
        return

    transect_row = transect_rows.iloc[-1]

    transect_start = pd.to_datetime(
        transect_row["transectStart"],
        utc=True,
    ).tz_convert(None)

    transect_end = pd.to_datetime(
        transect_row["transectEnd"],
        utc=True,
    ).tz_convert(None)

    # -----------------------------------------------------
    # Open already assembled CPS transect
    # -----------------------------------------------------

    ds_CPS = xr.open_zarr(
        latest_cps,
        consolidated=True,
    )

    ds_CPS = _prepare_sv_for_echogram(
        ds_CPS,
        var_name="Sv_masked",
    )

    ds_CPS.attrs.update(
        {
            "transect_number": str(
                transect_number
            ),
            "transect_start": str(
                transect_start
            ),
            "transect_end": str(
                transect_end
            ),
            "source_cps_file": (
                latest_cps.name
            ),
        }
    )

    # -----------------------------------------------------
    # Save visualization cache
    # -----------------------------------------------------

    output_path = (
        path_cache
        / file_CPS_zarr
    )

    print(
        f"Saving transect "
        f"{transect_number} "
        f"to CPS visualization cache: "
        f"{output_path}"
    )

    ds_CPS.chunk(
        {
            "channel": 1,
            "ping_time": -1,
            "echo_range": 4000,
        }
    ).to_zarr(
        output_path,
        mode="w",
        consolidated=True,
    )