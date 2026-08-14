from pathlib import Path

import holoviews as hv
import panel as pn
import xarray as xr
from holoviews.operation.datashader import rasterize

import datetime

hv.extension("bokeh")
pn.config.autoreload = False

path_CPS = Path(
    r"C:\Users\lloyd\Desktop\my_echopype\run_echopype_tests\echodataflow_edge_test_SH2407\viz_cache_CPS"
)


def plot_sv(
    ds: xr.Dataset,
    var_name: str,
    channel: str,
    title: str,
    vmin: float = -70,
    vmax: float = -36,
):
    """Plot Sv using the original irregular ping times."""

    da = ds[var_name].sel(channel=channel)

    quadmesh = hv.QuadMesh(
        (
            ds["ping_time"].values,
            ds["echo_range"].values,
            da.values.T,
        ),
        kdims=["ping_time", "echo_range"],
        vdims=["Sv"],
    )

    return rasterize(
        quadmesh,
        width=1000,
        height=400,
    ).opts(
        cmap="viridis",
        clim=(vmin, vmax),
        invert_yaxis=True,
        width=1000,
        height=400,
        tools=[
            "hover",
            "pan",
            "box_zoom",
            "wheel_zoom",
            "reset",
        ],
        title=title,
    )


def update_cache_cps():
    """Load the latest completed CPS transect and create echograms."""

    cache_path = path_CPS / "latest_CPS.zarr"

    ds = xr.open_zarr(
        cache_path
    )

    target_channel = "WBT 400142-15 ES70-7C_ES"

    transect_number = ds.attrs.get(
        "transect_number",
        "unknown",
    )

    cache_time = datetime.datetime.fromtimestamp(
        cache_path.stat().st_mtime
    ).strftime("%H:%M:%S")

    original = plot_sv(
        ds,
        var_name="Sv",
        channel=target_channel,
        title=(
            f"Original Sv - 70 kHz | "
            f"Transect {transect_number} | "
            f"cache {cache_time}"
        ),
    )

    masked = plot_sv(
        ds,
        var_name="Sv_masked",
        channel=target_channel,
        title=(
            f"CPS masked Sv - 70 kHz | "
            f"Transect {transect_number} | "
            f"cache {cache_time}"
        ),
    )

    return [original, masked]


def cps_app():
    """Plot original and CPS-masked Sv with regular updates."""

    # Create initial plots
    plots = update_cache_cps()
    plot_pane = pn.Column(*plots)

    def scheduled_update():
        try:
            new_plots = update_cache_cps()
            plot_pane[:] = new_plots
            print("CPS plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled CPS update: {e}")

    pn.state.add_periodic_callback(
        scheduled_update,
        period=60 * 1000,  # 1 min for testing
    )

    return plot_pane


test_server = pn.serve(
    {
        "cps_echogram": cps_app,
    },
    port=1803,
    websocket_origin="*",
    admin=True,
    show=False,
    autoreload=False,
    keep_alive=40000,
    check_unused_sessions_milliseconds=30000,
)