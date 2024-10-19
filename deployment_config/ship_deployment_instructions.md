# Deployment Instructions

## Setting up the Environment

### Python Environment

1. Create and Activate Environment

    Start by creating a new conda, venv, or mamba environment and activate it.

2. Clone Echodataflow

    Clone the echodataflow repository to your local machine:
    ```bash
    git clone https://github.com/OSOceanAcoustics/echodataflow
    ```
3. Install Echodataflow in Editable Mode

    Navigate to the cloned repository and install the package in editable mode:
    ```bash
    cd echodataflow
    pip install -e .
    ```

### Echodataflow configuration
1. Connect to Prefect Account

    Ensure your machine is connected to a Prefect account, which could be either a local Prefect server or a [Prefect Cloud account](https://docs.prefect.io/3.0/manage/cloud/connect-to-cloud).
2. Initialize Echodataflow

    In your environment, run the following command to set up the basic configuration for Echodataflow:

    ```bash
    echodataflow init
    ```
3. Create YAML Configuration Files
    
    Set up the YAML configuration files for each service to be deployed. Reference the [sample config files](https://drive.google.com/drive/u/2/folders/1C2Hs3-SxWbYaE3xTo7RRqAg4I7fzponW) for guidance and check the [documentation](https://echodataflow.readthedocs.io/en/latest/configuration/datastore.html) for additional information.
4. Add Required YAML Files

    Place the following YAML files in a directory. These files are required for the current deployment on Lasker or Shimada, if your use case is different feel free to modify the files accordingly:

    ```bash
    df_Sv_pipeline
    datastore.yaml
    pipeline.yaml
    datastore_MVBS.yaml
    pipeline_MVBS.yaml
    datastore_prediction.yaml
    pipeline_prediction.yaml
    ```

## Deploying the flows

1. Run Initial Scripts

    In the extensions folder of your environment, run `file_monitor.py` and `file_downloader.py`:

    ```bash
    python path/extensions/file_monitor.py
    python path/extensions/file_downloader.py
    ```
    Wait for the message "Your flow is being served and polling for scheduled runs!" to confirm that deployments have been created in your Prefect account.

2. Configure File Monitoring and File Transfer

    Configure the source for file monitoring and set up the `rclone` command for file transfer.

3. Run Main Deployment Script

    Run `main.py` from the deployment folder to create additional deployments: 

    ```bash
    python deployment/main.py
    ```
4. View and Edit Deployments in Prefect UI

    Go to the Prefect UI and check the Deployments tab to view the created deployments. You can duplicate deployments, modify schedules, and update datastore and pipeline configuration files directly from the UI.

5. Duplicate Deployments for Different Flows

    Create separate deployments for the Sv, MVBS, and prediction flows by duplicating the existing ones and customizing the schedule and configurations as needed.

6. Add Path to Configuration Files

    Update the deployment to include the correct paths to the YAML configuration files. If you're using S3 to manage config files, ensure to add the appropriate [block configuration](https://echodataflow.readthedocs.io/en/latest/configuration/blocks.html).

## Creating Work Pools and Work Queues
### Create Work Pools
In the Prefect UI, navigate to the Work Pools tab and create new pools. These pools can be distributed logically, such as one pool per service or per instance.

### Create Work Queues
Similarly, create work queues and assign them to the work pools. Distribute the queues in a way that optimizes load distribution across the available workers.  

## Spinning Up Workers
### Start Workers
Once the work pools and queues are set up, you can start the workers. Prefect will provide commands for each pool and queue, which you can run to spin up workers on the instance.

### Run Workers in Parallel
Each worker command should be executed in a separate terminal session. This will allow multiple workers to run in parallel, processing tasks across different flows simultaneously.


