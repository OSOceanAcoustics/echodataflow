import matplotlib
import panel as pn
import param
import diskcache as dc
import holoviews as hv
import xarray as xr
import echoshader
from holoviews import opts
import random
import echopype.colormap
import asyncio

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
cache = dc.Cache('~/eshader_cache_shimada')

zarr_path = cache.get('zarr_path')



def extract_frequency(channel):
    import re
    match = re.search(r'ES(\d+)', channel)
    return int(match.group(1)) if match else float('inf')

channel_tricolor = cache.get('channel_tricolor')

desired_order = [120, 38, 18]

channel_tricolor = sorted(channel_tricolor, key=lambda x: desired_order.index(extract_frequency(x)))

channel_multi_freq = cache.get('channel_multi_freq')

ek500_cmap = matplotlib.colormaps["ep.ek500"]

clipping = {
    'min': tuple(ek500_cmap.get_under()),
    'max': tuple(ek500_cmap.get_over()),
    'NaN': tuple(ek500_cmap.get_bad()),
}

from functools import partial

import numpy as np
import panel as pn

def update(ds_MVBS):
    temp = xr.open_zarr(zarr_path).compute()
    ds_MVBS["Sv"].data = temp["Sv"].data
 
print("Tri")
def tricolor_app():
    ds_MVBS = xr.open_zarr(zarr_path).compute()


   
    tricolor = ds_MVBS.eshader.echogram(
        channel=channel_tricolor,
        vmin=-70,
        vmax=-36,
        rgb_composite=True,
        opts=opts.RGB(width=800)
    )

    if ds_MVBS is not None and 'softmax' in list(ds_MVBS.keys()):
        hv_ds = hv.Dataset(ds_MVBS["softmax"])
        contours = hv.operation.contours(hv_ds.to(hv.Image, kdims=["ping_time", "echo_range"]), levels=[0.7, 0.8, 0.9])

    if 'softmax' in list(ds_MVBS.keys()):

        composed_plot = tricolor()*contours
        composed_plot = composed_plot.opts(opts.Contours(cmap='kgy', tools=['hover'], legend_position="bottom_right"))
        composed_plot = composed_plot.opts(xlabel='Ping Time')
        composed_plot = composed_plot.opts(ylabel='Depth (m)')
        plot = composed_plot
    else:
        tricolor = plot.opts(xlabel='Ping Time')
        tricolor = tricolor.opts(ylabel='Depth (m)')
        plot = tricolor
   
    return plot
    
# panel_object.tricolor[:] = tricolor_app

update_button = pn.widgets.Button(name='Refresh data')

def update_plot(event=None):
    return(tricolor_app())

view_tricolor = pn.Column(update_button,
          pn.panel(pn.bind(update_plot, update_button), loading_indicator=True),
       )
     


# multifrequency plot
print("Multi")
def multi_app():
    print("Multi")
    ds_MVBS = xr.open_zarr(zarr_path)

    if ds_MVBS is not None and 'softmax' in list(ds_MVBS.keys()):
        hv_ds = hv.Dataset(ds_MVBS["softmax"])
        contours = hv.operation.contours(hv_ds.to(hv.Image, kdims=["ping_time", "echo_range"]), levels=[0.7, 0.8, 0.9])
    
    if ds_MVBS is not None and channel_multi_freq is not None:
        egram = ds_MVBS.eshader.echogram(
               # channel=[channel_multi_freq[2],channel_multi_freq[0], channel_multi_freq[1]],
                vmin=-70,
                vmax=-36,
                cmap = "ep.ek500", 
                opts = opts.Image(clipping_colors=clipping, 
                                    width=800, 
                                    xlim=(ds_MVBS.ping_time.min().values, ds_MVBS.ping_time.max().values),
                                    xlabel='Depth (m)'
                )
        )
               

        if 'softmax' in list(ds_MVBS.keys()):
 
           plot = egram()*contours.opts(opts.Contours(cmap='rainbow', tools=['hover'], legend_position="bottom_right"))

        else:
           plot = egram()

    
    return plot

update_button = pn.widgets.Button(name='Refresh data')

def update_plot(event=None):   
    return(multi_app())

view_multi = pn.Column(update_button,
    pn.panel(pn.bind(update_plot, update_button), loading_indicator=True),
    )



# panel_object.multi_freq[:] = multi_app()


print("track")
def track_app():
    ds_MVBS = xr.open_zarr(zarr_path).compute()

    if ds_MVBS is not None:
        track = ds_MVBS.eshader.track(
            tile='EsriOceanBase',
            opts=hv.opts.Path(width=600, height=350)
        )
    return track

update_button = pn.widgets.Button(name='Refresh data')

def update_plot(event=None):   
    return(track_app())

view_track = pn.Column(update_button,
    pn.panel(pn.bind(update_plot, update_button), loading_indicator=True),
    )


#panel_object.track[:] = [track_app()]     


# print(panel_object.tile_select)
# if tile_select is not None:
#    panel_object.tile_select.options = tile_select.options
#    panel_object.tile_select.value = tile_select.value

#pn.serve({"tricolor": view}, port=1801, websocket_origin="*", admin=True, show=False)
pn.serve({"tricolor": view_tricolor, 'multi_freq': view_multi, 'track':view_track}, port=1801, websocket_origin="*", admin=True, show=False)


def all_app():
    ds_MVBS = xr.open_zarr(zarr_path).compute()
    d = {"tricolor": pn.Row(tricolor_app)}
    return(d)



