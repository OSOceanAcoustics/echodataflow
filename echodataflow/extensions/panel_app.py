import io
import matplotlib
import panel as pn
import param
import diskcache as dc
import holoviews as hv
import xarray as xr
import echoshader
import echopype.colormap
from holoviews import opts

class EchogramPanel(param.Parameterized):
    multi_freq = param.Parameter()
    tricolor = param.Parameter()
    track = param.Parameter()
    tile_select = param.Parameter()

    def __init__(self, **params):
        super().__init__(**params)
        self.multi_freq = pn.Column()
        self.tricolor = pn.Row()
        self.track = pn.Column()
        self.tile_select = pn.widgets.Select()

    @param.depends('multi_freq', 'tricolor', 'track', 'tile_select')
    def view(self):
        return {
            "multi_freq": pn.Column(self.multi_freq),
            "tricolor": pn.Row(self.tricolor),
            "track": pn.Column(self.tile_select, self.track),
        }

panel_object = EchogramPanel()

# Initialize DiskCache
cache = dc.Cache('~/eshader_cache')

def extract_frequency(channel):
    import re
    match = re.search(r'ES(\d+)', channel)
    return int(match.group(1)) if match else float('inf')

def update_panel_from_cache():
    global panel_object
    print("Updating panel from cache...")
    ds_MVBS = None
    try:
        if panel_object is None:
            panel_object = EchogramPanel()
        else:
            del panel_object
            panel_object = EchogramPanel()
        desired_order = [120, 38, 18]
        ek500_cmap = matplotlib.colormaps["ep.ek500"]
        clipping = {
            'min': tuple(ek500_cmap.get_under()),
            'max': tuple(ek500_cmap.get_over()),
            'NaN': tuple(ek500_cmap.get_bad()),
        }
        
        zarr_path = cache.get('zarr_path')
        channel_multi_freq = cache.get('channel_multi_freq')
        channel_tricolor = cache.get('channel_tricolor')
        tile_select = cache.get('tile_select')
        
        print(channel_multi_freq)
        print(channel_tricolor)
        print(tile_select)
        
        print(f'Loading data from {zarr_path}')
        
        print("tri")
        if zarr_path is not None:
            ds_MVBS = xr.open_zarr(zarr_path)
        if ds_MVBS is not None and channel_tricolor is not None:
            channel_tricolor = sorted(channel_tricolor, key=lambda x: desired_order.index(extract_frequency(x)))
            print(channel_tricolor)
            tricolor = ds_MVBS.eshader.echogram(
                channel=channel_tricolor,
                vmin=-70,
                vmax=-36,
                rgb_composite=True,
                opts=opts.RGB(width=800)
            )
            panel_object.tricolor[:] = [pn.pane.HoloViews(tricolor)]
        
        print("Multi")
        if ds_MVBS is not None and channel_multi_freq is not None:
            egram_all = []
            for ch in channel_tricolor:                
                egram = ds_MVBS.eshader.echogram(
                    channel=[ch],
                    vmin=-70,
                    vmax=-36,
                    cmap = "ep.ek500", 
                    opts = opts.Image(clipping_colors=clipping, width=800),
                )
                egram_all.append(egram)
            panel_object.multi_freq[:] = [pn.pane.HoloViews(e) for e in egram_all]
        
        print("track")
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
            del ds_MVBS

# Periodically update the panel
pn.state.add_periodic_callback(update_panel_from_cache, 138000)

update_panel_from_cache()

# Serve the Panel app
pn.serve(panel_object.view(), port=1800, websocket_origin="*", admin=True, show=False)
