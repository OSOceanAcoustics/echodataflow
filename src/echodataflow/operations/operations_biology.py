"""Biological data transformations and estimates for trawl hauls."""

from __future__ import annotations

import numpy as np
import pandas as pd

from echodataflow.utils.const import TS_L_PARAMS


def get_count_from_length_specimen(
    df_length: pd.DataFrame,
    df_specimen: pd.DataFrame,
) -> pd.DataFrame:
    """Combine length-frequency and specimen observations into length counts."""
    # Round fish length to nearest integer
    df_specimen["length"] = df_specimen["fork_length"].round(0).astype(int)

    specimen_counts = (
        df_specimen.groupby(["sex", "length", "haul"]).size().reset_index(name="frequency")
    )
    length_counts = (
        df_length.groupby(["sex", "length", "haul"])
        .agg({"frequency": "sum"})
        .reset_index()
    )

    df_combined = pd.merge(
        specimen_counts.reset_index(),
        length_counts.reset_index(),
        on=["sex", "length", "haul"],
        how="outer",
    ).fillna(0)
    df_combined["frequency"] = (
        df_combined["frequency_x"] + df_combined["frequency_y"]
    ).astype(int)

    return df_combined[["sex", "length", "frequency", "haul"]]


def get_length_weight_regression(df_specimen: pd.DataFrame) -> pd.DataFrame:
    """Get length-weight coefficients by sex and for all fish combined."""
    df_regres = (
        df_specimen.groupby(["sex", "stratum"])
        .apply(
            lambda df: pd.Series(
                np.polyfit(np.log10(df["length"]), np.log10(df["organism_weight"]), 1),
                index=["p1", "p2"],
            ),
            include_groups=False,
        )
        .reset_index()
    )

    df_all = (
        df_specimen.groupby("stratum")
        .apply(
            lambda df: pd.Series(
                np.polyfit(np.log10(df["length"]), np.log10(df["organism_weight"]), 1),
                index=["p1", "p2"],
            ),
            include_groups=False,
        )
        .reset_index()
    )
    df_all["sex"] = "all"

    df_regres = pd.concat([df_regres, df_all]).reset_index()
    df_regres["sex"] = df_regres["sex"].str.lower()
    return df_regres


def add_stratum(df: pd.DataFrame, df_stratum: pd.DataFrame) -> pd.DataFrame:
    """Assign strata based on each observation's latitude."""
    lat_bins = [-90.0] + df_stratum["latitude_northern_limit"].tolist() + [90.0]
    lat_labels = df_stratum["stratum"].tolist() + [max(df_stratum["stratum"]) + 1]

    df["stratum"] = pd.cut(
        df["latitude"],
        bins=lat_bins,
        labels=lat_labels,
        include_lowest=True,
    )
    return df


def get_sigma_bs_mean_stratum(df_length_count: pd.DataFrame) -> pd.DataFrame:
    """Compute the frequency-weighted mean backscattering cross-section by stratum."""
    df_length_count["sigma_bs"] = 10 ** (
        (
            TS_L_PARAMS["slope"] * np.log10(df_length_count["length"])
            + TS_L_PARAMS["intercept"]
        )
        / 10
    )

    return (
        df_length_count.groupby("stratum")
        .apply(
            lambda df: pd.Series(
                (df["sigma_bs"] * df["frequency"]).sum() / df["frequency"].sum(),
                index=["sigma_bs_mean"],
            ),
            include_groups=False,
        )
        .reset_index()
    )


def get_weight_mean_stratum(
    df_length_count: pd.DataFrame,
    df_length_weight_regression: pd.DataFrame,
) -> pd.DataFrame:
    """Compute frequency-weighted mean organism weight by stratum."""
    df_merged = pd.merge(
        df_length_count,
        df_length_weight_regression[df_length_weight_regression["sex"] == "all"][
            ["stratum", "p1", "p2"]
        ],
        on="stratum",
        how="left",
    )
    df_merged["weight"] = 10 ** (
        df_merged["p1"] * np.log10(df_merged["length"]) + df_merged["p2"]
    )

    return (
        df_merged.groupby("stratum")
        .apply(
            lambda df: pd.Series(
                (df["weight"] * df["frequency"]).sum() / df["frequency"].sum(),
                index=["weight_mean"],
            ),
            include_groups=False,
        )
        .reset_index()
    )
