import asyncio
from pathlib import Path

import dask_image.ndfilters
import echopype as ep
import echoregions as er
import numpy as np
import pandas as pd
import xarray as xr
from prefect import flow, get_client, runtime
from prefect.states import Cancelled
from echodataflow.flows.flows_helper import deployment_already_running
from prefect_dask import DaskTaskRunner

from echodataflow.utils.processing_ledger import get_completed_sv_files, resolve_database
from echodataflow.tasks.tasks_acoustics import (
    task_compute_NASC_from_masked_Sv,
)


def _pick_channel_by_frequency(
    ds: xr.Dataset,
    freq_hz: float,
) -> str:
    idx = int(
        np.abs(
            ds["frequency_nominal"].values - freq_hz
        ).argmin()
    )
    return str(ds["channel"].values[idx])


def _dilate_7x7(
    da: xr.DataArray,
) -> xr.DataArray:
    return xr.DataArray(
        dask_image.ndfilters.maximum_filter(
            da.data,
            size=(1, 7, 7),
        ),
        dims=da.dims,
        coords=da.coords,
    )


def _mask_above_seafloor(
    ds: xr.Dataset,
    bottom_line_path: Path,
    channel: str,
) -> xr.DataArray:
    lines = er.read_lines_csv(
        str(bottom_line_path)
    )

    depth_da = ds["depth"].sel(
        channel=channel
    )

    deepest_ping_idx = int(
        depth_da.max(
            dim="range_sample",
            skipna=True,
        )
        .argmax(dim="ping_time")
        .values
    )

    depth = (
        depth_da
        .isel(ping_time=deepest_ping_idx)
        .dropna(
            dim="range_sample",
            how="all",
        )
    )

    valid_length = depth.sizes[
        "range_sample"
    ]

    ds_trimmed = ds.isel(
        range_sample=slice(
            0,
            valid_length,
        )
    )

    sv_target = ds_trimmed["Sv"].sel(
        channel=[channel]
    )

    sv_for_regions = xr.DataArray(
        sv_target.values,
        dims=[
            "channel",
            "ping_time",
            "depth",
        ],
        coords={
            "channel": [channel],
            "ping_time": sv_target["ping_time"],
            "depth": depth.values,
        },
        name="Sv",
    )

    bottom_mask, _ = lines.seafloor_mask(
        sv_for_regions,
        operation="above_below",
        method="slinear",
        limit_area=None,
        limit_direction="both",
    )

    above_mask = ~bottom_mask.astype(bool)

    if "channel" in above_mask.dims:
        above_mask = above_mask.squeeze(
            "channel",
            drop=True,
        )

    above_mask = above_mask.rename(
        {"depth": "range_sample"}
    )

    above_mask = above_mask.assign_coords(
        range_sample=ds_trimmed[
            "range_sample"
        ]
    )

    return above_mask.reindex(
        range_sample=ds["range_sample"],
        fill_value=False,
    )


def _export_nasc_to_echoview_csv(
    ds_nasc: xr.Dataset,
    output_filepath: Path,
    process_id: int = 1928,
) -> None:
    df = (
        ds_nasc
        .to_dataframe()
        .reset_index()
    )

    depth_step = (
        float(
            np.nanmedian(
                np.diff(
                    ds_nasc["depth"].values
                )
            )
        )
        if ds_nasc.sizes.get(
            "depth",
            0,
        ) > 1
        else 5.0
    )

    ping_time = pd.to_datetime(
        df["ping_time"]
    )

    out = pd.DataFrame(
        {
            "Process_ID": process_id,
            "Interval": (
                pd.factorize(
                    df["distance"]
                )[0]
                + 1
            ),
            "Layer": (
                pd.factorize(
                    df["depth"]
                )[0]
                + 1
            ),
            "Sv_mean": -999.0,
            "NASC": df["NASC"].fillna(0.0),
            "Height_mean": depth_step,
            "Depth_mean": df["depth"],
            "Layer_depth_min": (
                df["depth"]
                - depth_step / 2
            ),
            "Layer_depth_max": (
                df["depth"]
                + depth_step / 2
            ),
            "Ping_S": 0,
            "Ping_E": 0,
            "Dist_M": (
                df["distance"] * 1852
            ),
            "Date_M": ping_time.dt.strftime(
                "%Y%m%d"
            ),
            "Time_M": (
                ping_time
                .dt.strftime(
                    "%H:%M:%S.%f"
                )
                .str[:-2]
            ),
            "Lat_M": df["latitude"],
            "Lon_M": df["longitude"],
            "Noise_Sv_1m": -999.0,
            "Minimum_Sv_threshold_applied": 1,
            "Maximum_Sv_threshold_applied": 0,
            "Standard_deviation": 0.0,
            "Thickness_mean": depth_step,
            "Range_mean": df["depth"],
            "Exclude_below_line_range_mean": 999.0,
            "Exclude_above_line_range_mean": 0.0,
        }
    )

    output_filepath.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    out.to_csv(
        output_filepath,
        index=False,
        encoding="utf-8-sig",
    )


@flow(
    log_prints=True,
    task_runner=DaskTaskRunner(),
)
def flow_process_CPS(
    path_transect_csv: str,
    path_snapshot_csv: str,
    path_main: str,
    processing_db: str = "processing.db",
    target_frequency: float = 70000,
    min_depth: float = 10.0,
    seafloor_threshold: list = [-40, 2.4, 1.0],
    seafloor_offset: float = 0.5,
    seafloor_r0: float = 10,
    seafloor_r1: float = 1000,
    seafloor_wtheta: int = 28,
    seafloor_wphi: int = 52,
    mask_mode: str = "cps",
    fallback_sv_threshold: float = -70,
    range_bin: str = "10m",
    dist_bin: str = "0.5nmi",
    nasc_process_id: int = 1928,
):

    # Prevent overlapping runs of this deployment
    already_running = asyncio.run(
        deployment_already_running()
    )

    if already_running:

        async def cancel_run():
            async with get_client() as client:
                await client.set_flow_run_state(
                    flow_run_id=runtime.flow_run.id,
                    state=Cancelled(
                        message=(
                            "Another instance of this "
                            "flow is already running"
                        )
                    ),
                )

        asyncio.run(cancel_run())
        return

    path_main = Path(path_main)

    path_transect = Path(
        path_transect_csv
    )

    path_snapshot = Path(
        path_snapshot_csv
    )

    path_sv = (
        path_main / "Sv"
    )

    db_path = resolve_database(path_main, processing_db)

    path_cps = (
        path_main / "CPS_Masks_Zarr"
    )

    path_bottom = (
        path_main / "CPS_Seafloor_CSVs"
    )

    path_nasc = (
        path_main / "CPS_NASC_Zarr"
    )

    path_nasc_csv = (
        path_main / "CPS_NASC_CSV"
    )

    for path in [
        path_cps,
        path_bottom,
        path_nasc,
        path_nasc_csv,
    ]:
        path.mkdir(
            parents=True,
            exist_ok=True,
        )

    # ---------------------------------------------
    # Find completed transects still needing CPS
    # ---------------------------------------------

    current = pd.read_csv(
        path_transect,
        dtype={
            "transectPart": "string",
            "transectNumber": "string",
            "transectStart": "string",
            "transectEnd": "string",
        },
    )

    # Ignore transects that have not finished yet.
    completed = current.dropna(
        subset=[
            "transectPart",
            "transectStart",
            "transectEnd",
        ]
    ).copy()

    pending_rows = []

    for _, transect in completed.iterrows():

        name = (
            f"transect_"
            f"{transect['transectPart']}"
        )

        cps_output = (
            path_cps
            / f"{name}_CPS.zarr"
        )

        nasc_output = (
            path_nasc
            / f"{name}_nasc.zarr"
        )

        # A transect is considered complete only when
        # both CPS and NASC products exist.
        if (
            cps_output.exists()
            and nasc_output.exists()
        ):
            continue

        pending_rows.append(
            transect
        )

    changed = pd.DataFrame(
        pending_rows,
        columns=current.columns,
    )

    if changed.empty:
        current.to_csv(
            path_snapshot,
            index=False,
        )
        print(
            "No completed transects require CPS processing."
        )
        return

    if isinstance(db_path, Path) and not db_path.exists():
        print(
            f"Processing ledger not found: "
            f"{db_path}"
        )
        return

    # ---------------------------------------------
    # Process each changed transect
    # ---------------------------------------------

    for _, transect in changed.iterrows():

        start = pd.to_datetime(
            transect["transectStart"],
            utc=True,
        )

        end = pd.to_datetime(
            transect["transectEnd"],
            utc=True,
        )

        name = (
            f"transect_"
            f"{transect['transectPart']}"
        )

        sv_filenames = get_completed_sv_files(
            db_path,
            start_time=start,
            end_time=end,
        )

        if not sv_filenames:
            print(
                f"No Sv data for {name}"
            )
            continue

        # -----------------------------------------
        # Build continuous Sv transect
        # -----------------------------------------

        sv_paths = [
            path_sv / filename
            for filename in sv_filenames
        ]

        sv_paths = [
            path
            for path in sv_paths
            if path.exists()
        ]

        if not sv_paths:
            continue

        datasets = [
            xr.open_zarr(path)
            for path in sv_paths
        ]

        ds = xr.concat(
            datasets,
            dim="ping_time",
            data_vars="minimal",
            coords="minimal",
            compat="override",
        ).sortby(
            "ping_time"
        )

        _, unique_idx = np.unique(
            ds["ping_time"].values,
            return_index=True,
        )

        ds = ds.isel(
            ping_time=np.sort(unique_idx)
        )

        if ds.sizes.get("ping_time", 0) == 0:
            continue

        # -----------------------------------------
        # Require complete Sv coverage
        # -----------------------------------------

        expected_start = start.tz_convert(None)
        expected_end = end.tz_convert(None)

        coverage_start = pd.Timestamp(
            ds["ping_time"].values[0]
        )

        coverage_end = pd.Timestamp(
            ds["ping_time"].values[-1]
        )

        tolerance = pd.Timedelta(seconds=5)

        if (
            coverage_start > expected_start + tolerance
            or coverage_end < expected_end - tolerance
        ):
            print(
                f"{name}: incomplete Sv coverage. "
                f"Available: {coverage_start} -> {coverage_end}; "
                f"required: {expected_start} -> {expected_end}. "
                "Leaving transect pending."
            )
            continue

        # We know the complete transect is now covered.
        ds = ds.sel(
            ping_time=slice(
                expected_start,
                expected_end,
            )
        )

        print(
            f"{name}: "
            f"{len(sv_paths)} Sv files, "
            f"{ds.sizes['ping_time']} pings"
        )

        # -----------------------------------------
        # CPS processing
        # -----------------------------------------

        chunks = {
            "channel": 1,
            "ping_time": 1000,
            "range_sample": -1,
        }

        target_channel = (
            _pick_channel_by_frequency(
                ds,
                target_frequency,
            )
        )

        chunked = ds.chunk(
            chunks
        )

        # -----------------------------------------
        # Common geometry
        # -----------------------------------------

        aligned = (
            ep.commongrid
            .resample_to_geometry(
                chunked,
                target_variable="Sv",
                target_channel=target_channel,
            )
        )

        if "sound_absorption" in ds:
            aligned[
                "sound_absorption"
            ] = ds[
                "sound_absorption"
            ]

        aligned = (
            ep.consolidate.add_depth(
                aligned
            )
        )

        ds[
            ["Sv", "echo_range"]
        ] = aligned[
            ["Sv", "echo_range"]
        ]

        ds = (
            ep.consolidate.add_depth(
                ds
            )
        )

        # -----------------------------------------
        # Background noise
        # -----------------------------------------

        try:
            ds = (
                ep.clean
                .remove_background_noise(
                    ds,
                    ping_num=20,
                    range_sample_num=5,
                    SNR_threshold="5.0dB",
                )
            )

        except Exception as exc:
            print(
                f"{name}: background-noise "
                f"removal failed: {exc}"
            )

            ds["Sv_corrected"] = (
                ds["Sv"]
            )

        sv_var = (
            "Sv_corrected"
            if "Sv_corrected" in ds
            else "Sv"
        )

        # -----------------------------------------
        # Detect seafloor with Blackwell
        # -----------------------------------------

        bottom_path = None

        try:
            bottom = (
                ep.mask.detect_seafloor(
                    ds=ds,
                    method="blackwell",
                    params={
                        "channel": (
                            target_channel
                        ),
                        "var_name": "Sv",
                        "threshold": (
                            seafloor_threshold
                        ),
                        "offset": (
                            seafloor_offset
                        ),
                        "r0": (
                            seafloor_r0
                        ),
                        "r1": (
                            seafloor_r1
                        ),
                        "wtheta": (
                            seafloor_wtheta
                        ),
                        "wphi": (
                            seafloor_wphi
                        ),
                    },
                )
            )

            bottom_df = pd.DataFrame(
                {
                    "time": (
                        bottom[
                            "ping_time"
                        ].values
                    ),
                    "depth": (
                        bottom.values
                    ),
                }
            )

            bottom_df = (
                bottom_df[
                    bottom_df[
                        "depth"
                    ] > -0.2
                ]
            )

            bottom_path = (
                path_bottom
                / f"{name}_bottom_line.csv"
            )

            bottom_df.to_csv(
                bottom_path,
                index=False,
            )

        except Exception as exc:
            print(
                f"{name}: seafloor "
                f"detection failed: {exc}"
            )

        # -----------------------------------------
        # Build valid water-column mask
        #
        # 1. Exclude surface <= min_depth
        # 2. Exclude seafloor and everything below
        #
        # This happens BEFORE CPS classification.
        # -----------------------------------------

        target_depth = (
            ds["depth"].sel(
                channel=target_channel
            )
        )

        surface_mask = (
            target_depth > min_depth
        )

        # If seafloor detection/masking fails,
        # keep the entire water column apart from
        # the surface exclusion.
        above_seafloor_mask = (
            xr.ones_like(
                surface_mask,
                dtype=bool,
            )
        )

        if bottom_path is not None:
            try:
                above_seafloor_mask = (
                    _mask_above_seafloor(
                        ds,
                        bottom_path,
                        target_channel,
                    )
                )

            except Exception as exc:
                print(
                    f"{name}: echoregions "
                    f"seafloor mask failed: {exc}"
                )

        valid_water_column = (
            surface_mask
            & above_seafloor_mask
        )
        
        # Save intermediate masks/products for diagnostics
        ds["surface_mask"] = surface_mask
        ds["above_seafloor_mask"] = above_seafloor_mask
        ds["valid_water_column"] = valid_water_column

        ds["Sv_water_column"] = (
            ds["Sv"].where(valid_water_column)
        )

        # Broadcast the 2-D water-column mask
        # (ping_time, range_sample) over channels.
        #
        # CPS calculations below therefore never
        # see the upper 10 m or the seafloor.
        sv_for_cps = (
            ds[sv_var].where(
                valid_water_column
            )
        )

        # -----------------------------------------
        # CPS classifier
        # -----------------------------------------

        try:

            # Smooth ONLY the valid water column
            ds["Sv_smoothed"] = (
                sv_for_cps
                .rolling(
                    ping_time=3,
                    range_sample=11,
                )
                .mean()
            )

            # Variance using the already-masked
            # water-column Sv
            ds["variance"] = (
                10 ** (
                    sv_for_cps / 10
                )
                - 10 ** (
                    ds["Sv_smoothed"]
                    / 10
                )
            ) ** 2

            ds["variance_smoothed"] = (
                ds["variance"]
                .rolling(
                    ping_time=3,
                    range_sample=11,
                )
                .mean()
            )

            ds["variance_smoothed"] = (
                10
                * np.log10(
                    ds[
                        "variance_smoothed"
                    ]
                    ** 0.5
                )
            )

            ds["variance_smoothed"] = (
                _dilate_7x7(
                    ds[
                        "variance_smoothed"
                    ]
                )
            )

            if (
                mask_mode == "cps"
                and ds.sizes[
                    "channel"
                ] >= 4
            ):

                ch38 = (
                    _pick_channel_by_frequency(
                        ds,
                        38000,
                    )
                )

                ch70 = (
                    _pick_channel_by_frequency(
                        ds,
                        70000,
                    )
                )

                ch120 = (
                    _pick_channel_by_frequency(
                        ds,
                        120000,
                    )
                )

                ch200 = (
                    _pick_channel_by_frequency(
                        ds,
                        200000,
                    )
                )

                # -----------------------------
                # Variance criteria
                # -----------------------------

                sd200 = (
                    ds[
                        "variance_smoothed"
                    ].sel(
                        channel=ch200
                    )
                )

                sd120 = (
                    ds[
                        "variance_smoothed"
                    ].sel(
                        channel=ch120
                    )
                )

                mask_sd = (
                    (sd200 > -65)
                    & (sd120 > -65)
                )

                # -----------------------------
                # Frequency-response criteria
                #
                # This is now calculated AFTER
                # surface + bottom removal.
                # -----------------------------

                ds["Sv_dilated"] = (
                    _dilate_7x7(
                        ds["Sv_smoothed"]
                    )
                )

                diff = (
                    ds["Sv_dilated"]
                    - ds[
                        "Sv_dilated"
                    ].sel(
                        channel=ch38
                    )
                )

                mask_frequency = (
                    (
                        diff.sel(
                            channel=ch200
                        )
                        > -13.51
                    )
                    & (
                        diff.sel(
                            channel=ch200
                        )
                        < 12.53
                    )
                    & (
                        diff.sel(
                            channel=ch120
                        )
                        > -13.50
                    )
                    & (
                        diff.sel(
                            channel=ch120
                        )
                        < 9.37
                    )
                    & (
                        diff.sel(
                            channel=ch70
                        )
                        > -13.85
                    )
                    & (
                        diff.sel(
                            channel=ch70
                        )
                        < 9.89
                    )
                )

                final_mask = (
                    mask_frequency
                    & mask_sd
                    & valid_water_column
                )

            else:

                # Fallback also uses ONLY the
                # valid water column.
                final_mask = (
                    sv_for_cps
                    > fallback_sv_threshold
                )

        except Exception as exc:

            print(
                f"{name}: CPS classifier "
                f"failed: {exc}"
            )

            # Same rule for fallback:
            # surface and bottom remain excluded.
            final_mask = (
                sv_for_cps
                > fallback_sv_threshold
            )

        # -----------------------------------------
        # Apply final CPS mask
        #
        # Values come from original Sv.
        # Classification was performed on
        # pre-masked / cleaned water-column Sv.
        # -----------------------------------------

        ds["Sv_masked"] = (
            ds["Sv"].where(
                final_mask
            )
        )

        # -----------------------------------------
        # Save CPS product
        # -----------------------------------------

        cps_path = (
            path_cps
            / f"{name}_CPS.zarr"
        )

        for variable in ds.variables:
            ds[
                variable
            ].encoding.pop(
                "chunks",
                None,
            )

        ds.chunk(
            chunks
        ).to_zarr(
            cps_path,
            mode="w",
            consolidated=True,
        )

        # -----------------------------------------
        # NASC
        #
        # Sv_masked already contains ONLY:
        #
        #   depth > min_depth
        #   above seafloor
        #   CPS-positive samples
        #
        # Therefore no additional surface or
        # bottom masking is necessary here.
        # -----------------------------------------

        ds_nasc = (
            task_compute_NASC_from_masked_Sv(
                ds_Sv_masked=ds,
                range_bin=range_bin,
                dist_bin=dist_bin,
            )
        )

        nasc_path = (
            path_nasc
            / f"{name}_nasc.zarr"
        )

        ds_nasc.to_zarr(
            nasc_path,
            mode="w",
            consolidated=True,
        )

        _export_nasc_to_echoview_csv(
            ds_nasc,
            path_nasc_csv
            / f"{name}_nasc.csv",
            process_id=nasc_process_id,
        )

        print(
            f"{name}: CPS + NASC complete"
        )

    current.to_csv(
        path_snapshot,
        index=False,
    )