from pathlib import Path
import datetime
import pandas as pd

import echopype as ep

from prefect import deploy, flow, task, get_run_logger
from prefect.variables import Variable

import boto3
from botocore import UNSIGNED
from botocore.config import Config

# Set up paths
data_path = Path("/Users/wujung/code_git/echodataflow/temp_data")
raw_path = data_path / "raw"
Sv_path = data_path / "Sv"

# Initiate counter for raw file copy
df_raw = pd.read_csv(
    data_path / "SH2306_raw_files.txt",
    # Path(__file__).parent / "../../temp_raw/SH2306_raw_files.txt", 
    sep=r'\s+',  # multiple spaces as delimiter
    header=None,  # no header
    names=["date", "time", "size", "filename"]  # assign column names
)

# Info dataframe for which raw files to process
Sv_csv_path = data_path / "SH2306_Sv_files.csv"
if not Sv_csv_path.exists():
    df_Sv = pd.DataFrame(
        columns=["raw_filename", "Sv_filename", "last_ping_time", "first_ping_time"]
    )
    df_Sv.to_csv(Sv_csv_path)



@flow(log_prints=True)
def copy_raw_files():
    print("Copy raw files to simulate data generation")
    print(f"Executed at {datetime.datetime.now(datetime.UTC)}")
    counter = Variable.get("counter_raw_copy")

    # Get filename from dataframe
    filename = df_raw.iloc[counter]['filename']
    print(f"Copying file #{counter}: {filename}")

    # Configure anonymous access
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    # Copy file
    bucket = "noaa-wcsd-pds"
    s3_path = f"data/raw/Bell_M._Shimada/SH2306/EK80/{filename}"    
    s3.download_file(bucket, s3_path, raw_path / f"{filename}")

    # Increment ocunter
    Variable.set("counter_raw_copy", value=counter+1, overwrite=True)


@flow(log_prints=True)
def process_raw_files():
    # Load info dataframe containing raw to Sv correspondence
    df_Sv = pd.read_csv(Sv_csv_path, index_col=0)
    raw_files_in_folder = set([ff.name for ff in raw_path.glob("*.raw")])
    raw_files_in_df = set(df_Sv["raw_filename"].tolist())

    # Find new files to process
    new_files = raw_files_in_folder.difference(raw_files_in_df)
    print(f"Found {len(new_files)} new files to process")

    # Convert raw files to Sv
    new_entries = []
    for nf in new_files:
        Sv_filename, first_ping_time, last_ping_time = raw2Sv(raw_path / nf)
        new_entries.append([nf, Sv_filename, first_ping_time, last_ping_time])

    # Add new entries to df_Sv
    if len(new_files) > 0:
        df_new = pd.DataFrame(
            new_entries,
            columns=["raw_filename", "Sv_filename", "last_ping_time", "first_ping_time"]
        )
        
        # Concatenate with existing df_Sv and save
        df_Sv = pd.concat([df_Sv, df_new], ignore_index=True)
        df_Sv.to_csv(Sv_csv_path)
        print(f"Added {len(new_files)} new entries to tracking CSV")


@task(log_prints=True)
def raw2Sv(raw_path: str):
    """
    Convert raw sonar data to Sv and save to zarr format.
    """
    # Convert raw file, consolidate Sv and save to zarr
    ed = ep.open_raw(
        raw_file=raw_path,
        sonar_model="EK80",  # can be raw_kwargs
    )

    # Compute Sv and consolidate depth and location
    ds_Sv = ep.calibrate.compute_Sv(
        echodata=ed,
        waveform_mode="CW",  # can be Sv_kwargs
        encode_mode="power",
    )
    ds_Sv = ep.consolidate.add_depth(
        ds=ds_Sv,
        depth_offset=9.5,  # can be depth_kwargs
    )
    ed["Platform"] = ed["Platform"].drop_duplicates("time1")
    ds_Sv = ep.consolidate.add_location(
        ds=ds_Sv,
        echodata=ed,
        nmea_sentence="GGA",  # can be location_kwargs
    )
    
    # Save to zarr
    out_path = Sv_path / f"{raw_path.stem}_Sv.zarr"
    ds_Sv.to_zarr(
        store=out_path,
        mode="w",
        consolidated=True,
        # storage_options=config.output.storage_options_dict,
    )
    first_ping_time = ds_Sv["ping_time"][-1].values
    last_ping_time = ds_Sv["ping_time"][0].values

    return out_path.name, first_ping_time, last_ping_time


@flow(log_prints=True)
def flow_1():
    print("This is flow 1")
    print(f"Executed at {datetime.datetime.now(datetime.UTC)}")


@flow(log_prints=True)
def flow_2():
    logger = get_run_logger()
    logger.info("This message will appear in the Prefect UI")    
    print("This is flow 2")
    print(f"Executed at {datetime.datetime.now(datetime.UTC)}")


@flow(log_prints=True)
def flow_3():
    print("This is flow 3")
    print(f"Executed at {datetime.datetime.now(datetime.UTC)}")

if __name__ == "__main__":
    # for seq, f in enumerate([flow_1, flow_2, flow_3]):
    #     freq_min = 15+seq
    #     f.from_source(
    #         source=".",
    #         entrypoint="/Users/wujung/code_git/echodataflow/echodataflow/rewrite/example.py:" + f.__name__
    #     ).deploy(
    #         name=f"{f.__name__}-deployment",
    #         work_pool_name="local",
    #         cron=f"*/{freq_min} * * * *"  # run every 5+seq mins
    #     )
    freq_min = 2
    deploy(
        copy_raw_files.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:copy_raw_files"
        ).to_deployment(
            name="copy-raw-files",
            # work_pool_name="local",
            # cron=f"*/{freq_min} * * * *",  # run every freq_min
        ),
        process_raw_files.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:process_raw_files"
        ).to_deployment(name="process-raw-files"),
        flow_3.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_3"
        ).to_deployment(name="flow_3-deploy"),
        work_pool_name="local",
    )
