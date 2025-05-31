from pathlib import Path
import datetime
import pandas as pd

from prefect import deploy, flow, get_run_logger
from prefect.variables import Variable

import boto3
from botocore import UNSIGNED
from botocore.config import Config


# Set up paths
data_path = Path("/Users/wujung/code_git/echodataflow/temp_data")
raw_path = data_path / "raw"
Sv_path = data_path / "Sv"
MVBS_path = data_path / "MVBS"
prediction_path = data_path / "prediction"


# Initiate counter for raw file copy
df_raw = pd.read_csv(
    "/Users/wujung/code_git/echodataflow/temp_data/SH2306_raw_files.txt",
    # Path(__file__).parent / "../../temp_raw/SH2306_raw_files.txt", 
    sep=r'\s+',  # multiple spaces as delimiter
    header=None,  # no header
    names=["date", "time", "size", "filename"]  # assign column names
)

@flow(log_prints=True)
def copy_raw():
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
        copy_raw.from_source(
            source=str(Path(__file__).parent),
            entrypoint="logging_test.py:copy_raw"
        ).to_deployment(
            name="copy-raw",
            # work_pool_name="local",
            # cron=f"*/{freq_min} * * * *",  # run every freq_min
        ),
        flow_2.from_source(
            source=str(Path(__file__).parent),
            entrypoint="logging_test.py:flow_2"
        ).to_deployment(name="flow_2-deploy"),
        flow_3.from_source(
            source=str(Path(__file__).parent),
            entrypoint="logging_test.py:flow_3"
        ).to_deployment(name="flow_3-deploy"),
        work_pool_name="local",
    )