from pathlib import Path

import xarray as xr
import panel as pn
from holoviews import opts

import echoshader


path_MVBS = Path("/media/volume/shimada_202506_volume/viz_data_cache")


def plot_multi_freq():
    """
    Plot multi-frequency echogram using echoshader.
    """
    ds_MVBS = xr.open_zarr(path_MVBS / "latest_MVBS.zarr")

    egram = ds_MVBS.eshader.echogram(
        channel=[
            "WBT 400141-15 ES18_ES",
            "WBT 400143-15 ES38B_ES",
            "WBT 400142-15 ES70-7C_ES",
            "WBT 400140-15 ES120-7C_ES",
            "WBT 400145-15 ES200-7C_ES",
        ],
        vmin=-70,
        vmax=-36,
        cmap = "viridis",
        opts = opts.Image(
            width=800, height=400,
            tools=["pan", "box_zoom", "wheel_zoom", "reset"],
        )
    )
    return egram


def update_plot(event=None):
    """
    Update the multi-frequency echogram plot.
    """
    return(plot_multi_freq())


# Add an update button to refresh the plot
update_button = pn.widgets.Button(name='Refresh data')


# Assemble panel
view_multi_freq = pn.Column(
    update_button,
    pn.panel(pn.bind(update_plot, update_button), loading_indicator=True),
)

# Deploy!
test_server = pn.serve(
    {"multi_freq_echogram": view_multi_freq},
    port=1802,
    websocket_origin="*",
    admin=True,
    show=False,
)

# test_server.stop()
