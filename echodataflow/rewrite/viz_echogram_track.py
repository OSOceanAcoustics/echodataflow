from pathlib import Path

import xarray as xr
import panel as pn
from holoviews import opts

import echoshader


path_MVBS = Path("/media/volume/shimada_202506_volume/viz_data_cache")
MVBS_files = sorted(list(path_MVBS.glob("MVBS_*.zarr")))

ds_MVBS = xr.open_zarr(MVBS_files[0])
ds_MVBS["echo_range"] = ds_MVBS["depth"]
ds_MVBS = ds_MVBS.swap_dims({"depth": "echo_range"})
ds_MVBS["Sv"] = ds_MVBS["Sv"].assign_attrs(
    actual_range=(float(ds_MVBS["Sv"].min().compute()),
                  float(ds_MVBS["Sv"].max().compute()))
)

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

test_server = pn.serve(
    {"test": pn.Column(egram())},
    port=1802,
    websocket_origin="*",
    admin=True,
    show=False,
)

# test_server.stop()
