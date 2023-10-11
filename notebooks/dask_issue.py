from prefect_dask import DaskTaskRunner
from prefect import task, flow

@task
def process_raw():
    temp_file = Path()
    raw = {'instrument': 'EK60', 'file_path': 's3://ncei-wcsd-archive/data/raw/Bell_M._Shimada/SH1707/EK60/Summer2017-D20170627-T203931.raw', 'transect_num': 7, 'month': 6, 'year': 2017, 'jday': 178, 'datetime': '2017-06-27T20:39:31'}