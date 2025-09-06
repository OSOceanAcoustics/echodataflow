from pathlib import Path
import datetime

import numpy as np
import pandas as pd
import geopandas as gpd
import shapely

from bokeh.models import HoverTool
import holoviews as hv
import geoviews as gv
import geoviews.tile_sources as gvts
import panel as pn


from viz_core import (
    THEME, BIO_VAR_NAME, COLORBAR_LABEL, BIO_VAR_UNIT
)


# Initialize extensions
hv.extension("bokeh", enable_mathjax=True)
hv.renderer("bokeh").theme = THEME
pn.extension()


# Path to data files
path_vm_local = Path("/media/volume/shimada_202506_volume/integration")
file_grid = path_vm_local / "grid_cells.geojson"
file_length_count = path_vm_local / "length_count_all.csv"


# # Define length count app widgets
# length_count_text = pn.pane.Markdown(
#     f"Length histograms last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
# )

# # Hidden widget for forcing plot refresh
# refresh_button_length_count = pn.widgets.Button(name="Refresh", visible=False)


# @pn.depends(refresh_button_length_count)
# def plot_length_count(refresh) -> hv.Overlay:
#     """
#     Plot length count from all haul catches.
#     """
#     # Load data
#     df_len_cnt_all = pd.read_csv(file_length_count)

#     # Define stratum options and colors
#     stratum_options = sorted(df_len_cnt_all['stratum'].dropna().unique())
#     stratum_colors = hv.Cycle('Category10').values
#     stratum_colors = {stratum: stratum_colors[i % len(stratum_colors)] for i, stratum in enumerate(stratum_options)}

#     # Get all values in "sex"
#     sex_options = df_len_cnt_all["sex"].unique().tolist()

#     # Create overlay
#     plots = []
#     for sex in sex_options:
#         overlay = None
#         for i, stratum in enumerate(stratum_options):
#             df_sub = df_len_cnt_all[(df_len_cnt_all['sex'] == sex) & (df_len_cnt_all['stratum'] == stratum)]
#             if not df_sub.empty:
#                 hist = hv.Histogram(np.histogram(df_sub['length'], bins=np.arange(0, 101, 5)), label=f"Stratum: {stratum}").opts(
#                     color=stratum_colors[stratum],
#                     alpha=0.6,  # default transparency
#                     muted_alpha=0.1,  # transparency when muted via legend
#                     height=250, width=500,
#                 )
#                 overlay = hist if overlay is None else overlay * hist
#         if overlay:
#             overlay = overlay.opts(title=f"Sex: {sex}", legend_position='right')
#             plots.append(overlay)
#     if plots:
#         return hv.Layout(plots).cols(1)
#     else:
#         return hv.Text(0, 0, "No data for available strata.")


# def length_count_app():
#     """
#     Application to visualize length count from all haul catches.
#     """
#     layout = pn.Column(
#         pn.pane.Markdown("# Length histograms"),
#         length_count_text, plot_length_count,
#         sizing_mode="stretch_width"
#     )

#     # Example scheduled update: change variable to trigger refresh
#     def scheduled_update():
#         try:
#             # Use hidden button to trigger plot_grid_map to run again
#             refresh_button_length_count.click()  # This triggers plot_grid_map to run again
#             length_count_text.object = (
#                 f"Length histograms last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
#             )
#             print("Plot updated at scheduled interval")
#         except Exception as e:
#             print(f"Error during scheduled update: {e}")

#     pn.state.add_periodic_callback(
#         scheduled_update,
#         period=2*60*1000  # Update every 2 mins
#     )

#     return layout


def clean_cells(
    gdf_grid_cells: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Clean POLYGON/MULTIPOLYGON objects
    """

    # Remove any POINT values
    gdf_polygon = gdf_grid_cells[
        gdf_grid_cells.geometry.apply(
            lambda x: isinstance(x, (shapely.geometry.Polygon, shapely.geometry.MultiPolygon))
        )
    ]

    return gdf_polygon


# Define widgets
grid_app_text = pn.pane.Markdown(
    f"Grid map last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
)

bio_var_selector = pn.widgets.Select(
    name="Biological estimate to plot",
    options=list(BIO_VAR_NAME.keys()),
    value="NASC"
)

# Get all WMTS tile source names
tile_options = [name for name, obj in gvts.__dict__.items() if isinstance(obj, gvts.WMTS)]

tile_selector = pn.widgets.Select(
    name="Basemap tile source",
    options=tile_options,
    value="EsriNatGeo"
)

# Hidden widget for forcing plot refresh
refresh_button = pn.widgets.Button(name="Refresh", visible=False)


@pn.depends(bio_var_selector, tile_selector, refresh_button)
def plot_grid_map(
    bio_var: str,
    map_tile: gv.element.geo.WMTS,
    refresh: bool,
) -> None:
    """
    Plot grid map according to the selected biological variable and tile source.
    """      
    # Load grid file
    gdf_grids = gpd.read_file(file_grid)
    gdf_grids = clean_cells(gdf_grids)

    # Get column name
    var = BIO_VAR_NAME.get(bio_var, "biomass")

    # Get the variable units
    var_units = BIO_VAR_UNIT.get(var, "biomass")

    # Get colorbar name/title
    colorbar_title = COLORBAR_LABEL.get(var, "biomass")

    # Get the base tilemap
    tile = getattr(gvts, map_tile)

    # Get data limits
    data_min, data_max = np.nanmin(gdf_grids[var]), np.nanmax(gdf_grids[var]) * 1.01
    
    # Add label column
    gdf_grids.loc[:, f"{var}_label"] = gdf_grids.loc[:, var].apply(
        lambda v: "Unsampled" if pd.isna(v) else f"{v:.2e} {var_units}"
    )
    
    # Update the hover tooltipe
    hover_tool = HoverTool(
        tooltips=[
            (f"{bio_var}", f"@{var}_label"),
            ("Area", "@area nmi²"),
        ]
    )

    # Begin plotting
    cells = gv.Polygons(
        gdf_grids, 
        vdims=[var, f"{var}_label", "area"]
    ).opts(
        width=900,
        height=800,
        colorbar=True,
        cmap="viridis",
        clim=(data_min, data_max),
        clipping_colors={"NaN": "white"},
        colorbar_opts={"title": colorbar_title},
        line_color="black",
        alpha=0.5,
        labelled=[bio_var],
        xlabel="Longitude (\u00B0E)",
        ylabel="Latitude (\u00B0N)",
        title=f"{bio_var}",
        tools=[hover_tool],
    )

    # Combine with tilemap
    overlay = tile * cells

    return overlay


def grid_app():
    """
    Application to visualize bio estimate grid cells.
    """
    layout = pn.Column(
        grid_app_text, bio_var_selector, tile_selector, plot_grid_map,
        sizing_mode="stretch_width"
    )
    # Example scheduled update: change variable to trigger refresh
    def scheduled_update():
        try:
            # # Test: cycle through variables
            # current = bio_var_selector.value
            # options = list(BIO_VAR_NAME.keys())
            # idx = (options.index(current) + 1) % len(options)
            # bio_var_selector.value = options[idx]  # triggers plot_grid_map to run

            # Use hidden button to trigger plot_grid_map to run again
            # refresh_button.click()  # This triggers plot_grid_map to run again
            refresh_button.param.trigger('value')
            grid_app_text.object = (
                f"Grid map last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            print("Plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")

    doc = pn.state.curdoc
    if not hasattr(doc, 'grid_map_callback'):
        doc.grid_map_callback = pn.state.add_periodic_callback(
            scheduled_update,
            period=1*60*1000  # Update every 1 mins
        )
        def cleanup(session_context):
            doc.grid_map_callback.stop()
        pn.state.on_session_destroyed(cleanup)
    # pn.state.add_periodic_callback(
    #     scheduled_update,
    #     period=2*60*1000  # Update every 2 mins
    # )

    return layout



# Deploy the application with stable configuration
test_server = pn.serve(
    {
        "biological_estimate_grid": grid_app,
        # "length_histograms": length_count_app,
    },
    port=1804,
    websocket_origin="*",
    admin=True,
    show=False,
    # Additional settings to prevent auto-refresh issues
    autoreload=False,
    # Keep WebSocket connection stable
    keep_alive=40000,  # 40 seconds
    check_unused_sessions_milliseconds=10000,  # check every 10 seconds
    # unused_session_lifetime=30000,             # kill unused after 30 seconds
)
