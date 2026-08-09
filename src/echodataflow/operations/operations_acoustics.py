"""Acoustic data-processing operations and their data contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import echopype as ep
import numpy as np
import pandas as pd
import xarray as xr


@dataclass(frozen=True)
class RawToSvWorkItem:
    """One raw sonar file to convert."""

    raw_path: str


@dataclass(frozen=True)
class RawToSvSettings:
    """Processing settings shared by a batch of raw files."""

    output_directory: str
    encode_mode: str = "power"
    waveform_mode: str = "CW"
    depth_offset: float = 9.5
    sonar_model: str = "EK80"
    datagram_type: str | None = None
    nmea_sentence: str | None = None


@dataclass(frozen=True)
class RawToSvResult:
    """Metadata describing one successfully created Sv store."""

    raw_filename: str
    sv_filename: str
    first_ping_time: pd.Timestamp
    last_ping_time: pd.Timestamp


def convert_raw_to_Sv(
    item: RawToSvWorkItem,
    settings: RawToSvSettings,
) -> RawToSvResult:
    """Convert one raw sonar file to a calibrated, consolidated Sv Zarr store."""
    raw_path = Path(item.raw_path)

    echodata = ep.open_raw(
        raw_file=raw_path,
        sonar_model=settings.sonar_model,
    )
    ds_sv = ep.calibrate.compute_Sv(
        echodata=echodata,
        waveform_mode=settings.waveform_mode,
        encode_mode=settings.encode_mode,
    )
    ds_sv = ep.consolidate.add_depth(
        ds=ds_sv,
        depth_offset=settings.depth_offset,
    )
    echodata["Platform"] = echodata["Platform"].drop_duplicates("time1")
    ds_sv = ep.consolidate.add_location(
        ds=ds_sv,
        echodata=echodata,
        datagram_type=settings.datagram_type,
        nmea_sentence=settings.nmea_sentence,
    )
    output_path = Path(settings.output_directory) / f"{raw_path.stem}_Sv.zarr"
    ds_sv.to_zarr(
        store=output_path,
        mode="w",
        consolidated=True,
    )

    return RawToSvResult(
        raw_filename=raw_path.name,
        sv_filename=output_path.name,
        first_ping_time=pd.to_datetime(ds_sv["ping_time"][0].values),
        last_ping_time=pd.to_datetime(ds_sv["ping_time"][-1].values),
    )


@dataclass(frozen=True)
class CreateMVBSWorkItem:
    """One time slice and its contributing Sv files."""

    start_time: pd.Timestamp
    end_time: pd.Timestamp
    sv_filenames: tuple[str, ...]
    mvbs_filename: str


@dataclass(frozen=True)
class CreateMVBSSettings:
    """Processing settings shared by a batch of MVBS slices."""

    sv_directory: str
    output_directory: str
    range_bin: str
    ping_time_bin: str


@dataclass(frozen=True)
class CreateMVBSResult:
    """Metadata describing one successfully created MVBS slice."""

    mvbs_filename: str
    first_ping_time: pd.Timestamp
    last_ping_time: pd.Timestamp


def create_MVBS(
    item: CreateMVBSWorkItem,
    settings: CreateMVBSSettings,
) -> CreateMVBSResult:
    """Create one MVBS time slice from its contributing Sv files."""
    # Remove timezone info for slicing
    start_time = item.start_time.replace(tzinfo=None)
    end_time = item.end_time.replace(tzinfo=None)

    # Combine Sv files into a single dataset
    ds_Sv = xr.open_mfdataset(
        [Path(settings.sv_directory) / svf for svf in item.sv_filenames],
        parallel=True,
        coords="minimal",
        data_vars="minimal",
        compat="override",
        chunks={"channel": 1, "ping_time": 1000, "range_sample": -1},
        engine="zarr",  # use zarr engine for reading
    ).sel(
        # slice start/end, end exclusive
        ping_time=slice(start_time, end_time - pd.to_timedelta("1nanoseconds"))
    )

    # Compute MVBS for the slice
    ds_MVBS = ep.commongrid.compute_MVBS(
        ds_Sv=ds_Sv,
        range_var="depth",
        range_bin=settings.range_bin,
        ping_time_bin=settings.ping_time_bin,
        reindex=False,
        fill_value=np.nan,
    )

    # Save to zarr: 1 chunk along each dimension
    ds_MVBS.chunk({"channel": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=Path(settings.output_directory) / item.mvbs_filename,
        mode="w",
        consolidated=True,
        # storage_options=config.output.storage_options_dict,
    )

    return CreateMVBSResult(
        mvbs_filename=item.mvbs_filename,
        first_ping_time=pd.to_datetime(ds_MVBS["ping_time"][0].values),
        last_ping_time=pd.to_datetime(ds_MVBS["ping_time"][-1].values),
    )
