from pathlib import Path
import pandas as pd

import configparser, s3fs

from echodataflow.operations.operations_biology import (
    add_stratum,
    get_count_from_length_specimen,
    get_length_weight_regression,
    get_sigma_bs_mean_stratum,
    get_weight_mean_stratum,
)
from echodataflow.utils.const import INFO_DATAFRAME_MAPPING
from echodataflow.utils.trawl_files import get_valid_hauls

from prefect import flow


@flow(log_prints=True)
def flow_ingest_haul(
    path_vm_local: str = "VM_LOCAL_PATH",
    path_bio_files: str = "BIO_CSV_CLOUD_LOCATION",
    cred_file: str = "CREDENTIAL_FILE",
    file_haul_info_all: str = "haul_info_all.csv",
    file_specimen_all: str = "specimen_all.csv",
    file_length_all: str = "length_all.csv",
    file_length_count_all: str = "length_count_all.csv",
    file_stratum_mean: str = "stratum_mean.csv",
):

    # Assemble full paths
    path_vm_local: Path = Path(path_vm_local)
    file_haul_info_all: Path = path_vm_local / file_haul_info_all
    file_specimen_all: Path = path_vm_local / file_specimen_all
    file_length_all: Path = path_vm_local / file_length_all
    file_length_count_all: Path = path_vm_local / file_length_count_all
    file_stratum_mean: Path = path_vm_local / file_stratum_mean

    # Get cloud bucket
    config = configparser.ConfigParser()
    config.read(cred_file)
    fs = s3fs.S3FileSystem(
        key=config["osn_sdsc_hake"]["access_key_id"],
        secret=config["osn_sdsc_hake"]["secret_access_key"],
        client_kwargs={"endpoint_url": config["osn_sdsc_hake"]["endpoint"]},
    )

    bio_filenames = {
        "length": fs.glob(f"{path_bio_files}/*/*_LFdata.xlsx"),
        "specimen": fs.glob(f"{path_bio_files}/*/*_specimens.xlsx"),
        "catch": fs.glob(f"{path_bio_files}/*/*_CatchPerc.xlsx"),
        "info": fs.glob(f"{path_bio_files}/*/*_NetConfig.xlsx"),
    }

    # Exist if no file present
    if not any(bio_filenames.values()):
        print(f"No biology files found.")
        return

    # Get complete haul inventories (those with all four file types)
    hauls_valid = get_valid_hauls(bio_filenames)
    print("Valid hauls:", list(hauls_valid))

    # Get hauls to process
    if not file_haul_info_all.exists():
        df_haul_info_all = pd.DataFrame()
        hauls_processed = set()
    else:            
        df_haul_info_all = pd.read_csv(file_haul_info_all, index_col=0)
        hauls_processed = set(df_haul_info_all["haul"].unique())
    hauls_to_process = sorted(set(hauls_valid).difference(hauls_processed))

    if not hauls_to_process:  # if there are hauls to process
        print(f"No hauls to process.")
        return
    else:
        print(
            f"Processing {len(hauls_to_process)} hauls for :\n",
            hauls_to_process
        )
        # Load dataframes from all hauls to process
        df_length = []
        df_specimen = []
        df_info = []
        for haul_num in hauls_to_process:
            haul_files = hauls_valid[haul_num]
            with fs.open(haul_files["length"]) as f:
                df_length_temp = pd.read_excel(f, index_col=0, sheet_name="Codend").reset_index().drop("Sum", axis=1)
                df_length_temp = df_length_temp.melt(
                    id_vars=["length"],
                    var_name="sex", 
                    value_name="frequency"
                ).assign(haul=haul_num)
                df_length_temp["frequency"] = df_length_temp["frequency"].fillna(0).astype(int)
                df_length.append(df_length_temp)
            with fs.open(haul_files["specimen"]) as f:
                df_specimen.append(
                    pd.read_excel(f, index_col=0, sheet_name="Codend")
                    .reset_index().assign(haul=haul_num)
                )
            with fs.open(haul_files["info"]) as f:
                df_info_temp = (
                    pd.read_excel(f, index_col=0, sheet_name="ButtonPresses").reset_index()
                    .rename(columns=INFO_DATAFRAME_MAPPING)
                )
                df_info.append(
                    # reset index to get haul number into a column
                    df_info_temp[df_info_temp["button"] == "NIW"][["haul", "timestamp", "latitude", "longitude"]]
                )
        df_length = pd.concat(df_length, ignore_index=True)
        df_specimen = pd.concat(df_specimen, ignore_index=True)
        df_info = pd.concat(df_info, ignore_index=True)

        # Combined length frequency from length and specimen dataframes
        df_length_count = get_count_from_length_specimen(df_length, df_specimen)

        # Add haul number and lat/lon for downstream stratification
        df_specimen = pd.merge(
            df_specimen,
            df_info,
            on="haul",
            how="left"
        )
        df_length = pd.merge(
            df_length,
            df_info,
            on="haul",
            how="left"
        )
        df_length_count = pd.merge(
            df_length_count,
            df_info,
            on="haul",
            how="left"
        )

        # Update df_haul_info_all, df_specimen_all, df_length_all
        df_specimen_all = pd.read_csv(file_specimen_all, index_col=0) if file_specimen_all.exists() else pd.DataFrame()
        df_length_all = pd.read_csv(file_length_all, index_col=0) if file_length_all.exists() else pd.DataFrame()
        df_length_count_all = pd.read_csv(file_length_count_all, index_col=0) if file_length_count_all.exists() else pd.DataFrame()
        df_haul_info_all = pd.read_csv(file_haul_info_all, index_col=0) if file_haul_info_all.exists() else pd.DataFrame()

        df_specimen_all = pd.concat([df_specimen_all, df_specimen], ignore_index=True)
        df_length_all = pd.concat([df_length_all, df_length], ignore_index=True)
        df_length_count_all = pd.concat([df_length_count_all, df_length_count], ignore_index=True)
        df_haul_info_all = pd.concat([df_haul_info_all, df_info], ignore_index=True)

        # Add stratrum info to df_specimen and df_length_count based on latitude
        df_stratum = pd.read_csv(
            Path(__file__).parent / "inpfc_def.csv", index_col=0
        ).reset_index().rename(columns={"stratum_num": "stratum"})
        df_specimen_all = add_stratum(df_specimen_all, df_stratum)
        df_length_all = add_stratum(df_length_all, df_stratum)
        df_length_count_all = add_stratum(df_length_count_all, df_stratum)
        df_haul_info_all = add_stratum(df_haul_info_all, df_stratum)

        # Save updated dataframes
        df_specimen_all.to_csv(file_specimen_all)
        df_length_all.to_csv(file_length_all)
        df_length_count_all.to_csv(file_length_count_all)
        df_haul_info_all.to_csv(file_haul_info_all)

        # Compute length-weight relationship for each stratum
        # Separately for: male, female, all fish combined
        df_length_weight_regression = get_length_weight_regression(df_specimen_all)

        # Compute mean sigma_bs and mean weight for each stratum
        # columns: stratum, sigma_bs_mean, weight_mean
        df_stratum = pd.merge(
            df_stratum,
            get_sigma_bs_mean_stratum(df_length_count_all),
            on="stratum",
            how="outer"
        )
        df_stratum = pd.merge(
            df_stratum,
            get_weight_mean_stratum(df_length_count_all, df_length_weight_regression),
            on="stratum",
            how="outer"
        )
        df_stratum.to_csv(file_stratum_mean)
