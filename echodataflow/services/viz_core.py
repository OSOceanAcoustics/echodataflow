from bokeh.themes.theme import Theme
import geoviews.tile_sources as gvts

from bokeh.themes.theme import Theme
import geoviews.tile_sources as gvts


# Plot theme
THEME = Theme(
    json={
        "attrs": {
            "Title": {
                "align": "left",
                "text_font_size": "20px",
                "text_color": "black",
            },
            "Axis": {
                "axis_label_text_font_style": "bold",
                "axis_label_text_color": "black",
                "axis_label_text_font_size": "18px",
                "major_label_text_font_size": "15px",
                "major_label_text_color": "black",
            },
            "ColorBar": {
                "title_text_font_style": "normal",
                "title_text_font_size": "18px",
                "title_text_color": "black",
                "major_label_text_font_size": "16px",
                "major_label_text_color": "black",
            },
            "Legend": {
                "title_text_font_style": "bold",
                "title_text_font_size": "16px",
                "title_text_color": "black",
            }
        }
    }
)


# Variable display map
BIO_VAR_NAME = {
    "NASC": "NASC",
    "Abundance": "abundance",
    "Biomass": "biomass",
    "Number density": "number_density",
    "Biomass density": "biomass_density"
}

# Colorbar title name map
COLORBAR_LABEL = {
    "NASC": r'$$\sum \mathrm{NASC}$$',
    "abundance": r"$$\sum N$$",
    "biomass": r"$$\sum \mathrm{kg}$$",
    "number_density": r"$$\overline{N~\mathrm{nmi^2}}$$",
    "biomass_density": r"$$\overline{\mathrm{kg~nmi^2}}$$",
}

# Variable units map
BIO_VAR_UNIT = {
    "NASC": "m² nmi⁻²",
    "abundance": "N",
    "biomass": "kg",
    "number_density": "N nmi⁻²",
    "biomass_density": "kg nmi⁻²",
}

# Tile source map
TILE_MAP = {
    "OpenTopoMap": gvts.OpenTopoMap,
    "OpenStreetMap": gvts.OSM,
    "ESRI NatGeo": gvts.EsriNatGeo,
    "ESRI Imagery": gvts.EsriImagery,
    "Carto Light": gvts.CartoLight,
    "Carto Dark": gvts.CartoDark,
}
