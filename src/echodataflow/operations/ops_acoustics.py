"""Acoustic data-processing operations and their data contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import echopype as ep
import pandas as pd


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
