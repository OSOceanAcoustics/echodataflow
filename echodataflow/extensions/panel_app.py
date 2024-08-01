import io
import panel as pn
import param
import diskcache as dc
import holoviews as hv
import xarray as xr
import echoshader

class EchogramPanel(param.Parameterized):
    multi_freq = param.Parameter()
    tricolor = param.Parameter()
    track = param.Parameter()
    tile_select = param.Parameter()

    def __init__(self, **params):
        super().__init__(**params)
        self.multi_freq = pn.Row()
        self.tricolor = pn.Row()
        self.track = pn.Column()
        self.tile_select = pn.widgets.Select()

    @param.depends('multi_freq', 'tricolor', 'track', 'tile_select')
    def view(self):
        return {
            "multi_freq": pn.Row(self.multi_freq),
            "tricolor": pn.Row(self.tricolor),
            "track": pn.Column(self.tile_select, self.track),
        }

panel_object = EchogramPanel()

# Initialize DiskCache
cache = dc.Cache('~/eshader_cache')

def update_panel_from_cache():
    print("Updating panel from cache...")
    ds_MVBS = None
    try:
        
        zarr_path = cache.get('zarr_path')
        channel_multi_freq = cache.get('channel_multi_freq')
        channel_tricolor = cache.get('channel_tricolor')
        tile_select = cache.get('tile_select')
        print(f'Loading data from {zarr_path}')
        if zarr_path is not None:
            ds_MVBS = xr.open_zarr(zarr_path)

        if ds_MVBS is not None and channel_multi_freq is not None:
            egram_all = ds_MVBS.sel(echo_range=slice(None, 400)).eshader.echogram(
                channel=channel_multi_freq,
                vmin=-70,
                vmax=-36,
                cmap="viridis",
                opts=hv.opts.Image(width=800)
            )
            panel_object.multi_freq[:] = [pn.pane.HoloViews(egram_all)]
        
        if ds_MVBS is not None and channel_tricolor is not None:
            tricolor = ds_MVBS.sel(echo_range=slice(None, 400)).eshader.echogram(
                channel=channel_tricolor,
                vmin=-70,
                vmax=-36,
                rgb_composite=True,
                opts=hv.opts.RGB(width=800)
            )
            panel_object.tricolor[:] = [pn.pane.HoloViews(tricolor)]
        
        if ds_MVBS is not None:
            track = ds_MVBS.eshader.track(
                tile='EsriOceanBase',
                opts=hv.opts.Path(width=600, height=350)
            )
            panel_object.track[:] = [pn.pane.HoloViews(track)]
        
        if tile_select is not None:
            panel_object.tile_select.options = tile_select.options
            panel_object.tile_select.value = tile_select.value

    except Exception as e:
        print(f"Error updating panel: {e}")
    finally:
        if ds_MVBS is not None:
            ds_MVBS.close()

# Periodically update the panel
pn.state.add_periodic_callback(update_panel_from_cache, 100000)

# Serve the Panel app
pn.serve(panel_object.view(), port=1800, websocket_origin="*", admin=True, show=True)
