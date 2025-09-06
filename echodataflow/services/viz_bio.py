from pathlib import Path
import panel as pn

import pandas as pd
import numpy as np
import geopandas as gpd
import shapely

import geoviews as gv
import geoviews.tile_sources as gvts
import holoviews as hv
import panel as pn
from bokeh.models import HoverTool

import datetime



from viz_core import (
    THEME, BIO_VAR_NAME, COLORBAR_LABEL, BIO_VAR_UNIT, TILE_MAP
)


# Initialize extensions
hv.extension("bokeh", enable_mathjax=True)
hv.renderer("bokeh").theme = THEME
pn.extension()


# Path to grid file
path_grid = Path("/media/volume/shimada_202506_volume/integrated")
file_grid = path_grid / "grid_cells.geojson"



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
info_text = pn.pane.Markdown(
    "# Bio Estimate Grid\nPanel last updated: never",
    sizing_mode="stretch_width"
)

variable_selector = pn.widgets.Select(
    name="Select variable",
    options=list(BIO_VAR_NAME.keys()),
    value="NASC"
)

# Get all WMTS tile source names
tile_options = [name for name, obj in gvts.__dict__.items() if isinstance(obj, gvts.WMTS)]

tile_selector = pn.widgets.Select(
    name="Basemap tile source",
    options=tile_options,
    value="OpenTopoMap"
)

@pn.depends(variable_selector, tile_selector)
def render_polygons(
    bio_var: str,
    map_tile: gv.element.geo.WMTS,
) -> None:
    """
    Panel plotting function
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
        # Dimensions
        width=900,
        height=800,
        # Polygon colors
        colorbar=True,
        # cmap=palette,
        clim=(data_min, data_max),
        clipping_colors={
            "NaN": "#c1c1c1",
        },
        colorbar_opts={
            "title": colorbar_title,
        },
        line_color="black",
        alpha=0.5,
        labelled=[bio_var],
        # Axis labels
        xlabel="Longitude (\u00B0E)",
        ylabel="Latitude (\u00B0N)",
        # Plot title
        title=f"{bio_var}",
        # Hover tooltip
        tools=[hover_tool],
    )

    # Combine with tilemap
    overlay = tile * cells

    return overlay


def grid_app():
    """
    Application to visualize bio estimate grid cells.
    """
    # layout = plot_grid()
    layout = pn.Column(
        info_text, variable_selector, tile_selector, render_polygons,
        sizing_mode="stretch_width"
    )

    # Example scheduled update: change variable to trigger refresh
    def scheduled_update():
        try:
            # Example: cycle through variables
            current = variable_selector.value
            options = list(BIO_VAR_NAME.keys())
            idx = (options.index(current) + 1) % len(options)
            variable_selector.value = options[idx]  # This triggers the plot update
            info_text.object = f"# Bio Estimate Grid\nPanel last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nCurrent variable: **{options[idx]}**"
            print("Plot updated at scheduled interval")
        except Exception as e:
            print(f"Error during scheduled update: {e}")

    pn.state.add_periodic_callback(
        scheduled_update,
        period=2*60*1000  # Update every 2 mins
    )

    return layout



# Deploy the application with stable configuration
test_server = pn.serve(
    {
        "biological_estimate_grid": grid_app,
    },
    port=1803,
    websocket_origin="*",
    admin=True,
    show=False,
    # Additional settings to prevent auto-refresh issues
    autoreload=False,
    # Keep WebSocket connection stable
    keep_alive=40000,  # 40 seconds
    check_unused_sessions_milliseconds=10000,  # check every 10 seconds
    unused_session_lifetime=30000,             # kill unused after 30 seconds
)
