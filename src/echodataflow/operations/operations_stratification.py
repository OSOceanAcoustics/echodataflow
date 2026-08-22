"""Shared survey stratification operations."""

from __future__ import annotations

import pandas as pd


def assign_stratum(
    dataframe: pd.DataFrame,
    stratum_definitions: pd.DataFrame,
) -> pd.DataFrame:
    """Return a copy with strata assigned from each observation's latitude."""
    result = dataframe.copy()

    # Extend the configured northern limits to cover every possible latitude
    latitude_bins = [-90.0] + stratum_definitions["latitude_northern_limit"].tolist() + [90.0]
    stratum_labels = stratum_definitions["stratum"].tolist() + [
        max(stratum_definitions["stratum"]) + 1
    ]
    result["stratum"] = pd.cut(
        result["latitude"],
        bins=latitude_bins,
        labels=stratum_labels,
        include_lowest=True,
    )
    return result
