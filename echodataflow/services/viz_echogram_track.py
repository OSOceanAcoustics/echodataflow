from pathlib import Path
import panel as pn
import xarray as xr
from holoviews import opts
import echoshader

# Configure Panel to prevent automatic refreshes
pn.config.autoreload = False

path_MVBS = Path("/media/volume/shimada_202506_volume/viz_data_cache")

def update_cache_MVBS():
    """Load latest MVBS data and create echogram"""
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
        cmap="viridis",
        opts=opts.Image(
            width=800, height=400,
            tools=["pan", "box_zoom", "wheel_zoom", "reset"],
        )
    )
    return egram

def multi_freq_app():
    """
    Plot multi-frequency echogram with regular updates.
    """
    # Create initial plot
    egram = update_cache_MVBS()
    plot_pane = pn.pane.HoloViews(egram)
    
    # Simple update function that only runs every 10 minutes
    def scheduled_update():
        try:
            new_egram = update_cache_MVBS()
            plot_pane.object = new_egram
            print("Plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")
    
    # Add ONLY the 10-minute callback - no other automatic updates
    pn.state.add_periodic_callback(
        scheduled_update,
        period=10*60*1000  # Update every 10 mins
    )
    
    return plot_pane

# Deploy the application with stable configuration
test_server = pn.serve(
    {"multi_freq_echogram": multi_freq_app},
    port=1802,
    websocket_origin="*",
    admin=True,
    show=False,
    # Additional settings to prevent auto-refresh issues
    autoreload=False,
    # Keep WebSocket connection stable
    keep_alive=40000,  # 40 seconds
    check_unused_sessions_milliseconds=30000,  # 30 seconds
)

# test_server.stop()
