# Echodataflow: Streamlined Data Pipeline Orchestration

Welcome to **Echodataflow**! Echodataflow is a powerful data pipeline orchestration tool designed to simplify and enhance the execution of data processing tasks. Leveraging the capabilities of [Prefect 2.0](https://www.prefect.io/) and YAML configuration files, Echodataflow caters to the needs of scientific research and data analysis. It provides an efficient way to define, configure, and execute complex data processing workflows.

Echodataflow integrates with [**Echopype**](https://github.com/OSOceanAcoustics/echopype), a renowned package for sonar data analysis, to provide a versatile solution for researchers, analysts, and engineers. With Echodataflow, users can seamlessly process and analyze sonar data using a modular and user-friendly approach.


# Getting Started with Echodataflow

This guide will walk you through the initial steps to set up and run your Echodataflow pipelines.

## 1. Create a Virtual Environment

To keep your Echodataflow environment isolated, it's recommended to create a virtual environment using Conda or Python's built-in `venv` module. Here's an example using Conda:

```bash
conda create --name echodataflow-env
conda activate echodataflow-env
```

Or, using Python's venv:

```bash
python -m venv echodataflow-env
source echodataflow-env/bin/activate  # On Windows, use `echodataflow-env\Scripts\activate`
```

## 2. Clone the Project
Now that you have a virtual environment set up, you can clone the Echodataflow project repository to your local machine using the following command:

```bash
git clone <repository_url>
```

## 3. Install the Package
Navigate to the project directory you've just cloned and install the Echodataflow package. The -e flag is crucial as it enables editable mode, which is especially helpful during development and testing. Now, take a moment and let the echodataflow do its thing while you enjoy your coffee.

```bash
cd <project_directory>
pip install -e .
```

## 4. Echodataflow and Prefect Initialization

To kickstart your journey with Echodataflow and Prefect, follow these simple initialization steps:

### 4.1 Initializing Echodataflow
Begin by initializing Echodataflow with the following command:

```bash
echodataflow init
```
This command sets up the groundwork for your Echodataflow environment, preparing it for seamless usage.

### 4.2 Initializing Prefect
For Prefect, initialization involves a few extra steps, including secure authentication. Enter the following command to initiate the Prefect authentication process:

- If you have a Prefect Cloud account, provide your Prefect API key to securely link your account. Type your API key when prompted and press Enter.

```bash
prefect cloud login
```

- If you don't have a Prefect Cloud account yet, you can use local prefect account. This is especially useful for those who are just starting out and want to explore Prefect without an account.

```bash
prefect profiles create echodataflow-local
```

The initialization process will ensure that both Echodataflow and Prefect are properly set up and ready for you to dive into your cloud-based workflows.

## 5. Configure Blocks
Echodataflow utilizes the concept of [blocks](./docs/configuration/blocks.md) which are secure containers for storing credentials and sensitive data. If you're running the entire flow locally, feel free to bypass this step.To set up your cloud credentials, configure blocks according to your cloud provider. For detailed instructions, refer to the [Blocks Configuration Guide](./docs/configuration/blocks.md#creating-credential-blocks).

## 6. Edit the Pipeline Configuration
Open the [pipeline.yaml](./docs/configuration/pipeline.md) file. This YAML configuration file defines the processes you want to execute as part of your pipeline. Customize it by adding the necessary stages and functions from echopype that you wish to run.

## 7. Define Data Sources and Destinations
Customize the [datastore.yaml](./docs/configuration/datastore.md) file to define the source and destination for your pipeline's data. This is where Echodataflow will fetch and store data as it executes the pipeline.

## 8. Execute the Pipeline
You're now ready to execute your Echodataflow pipeline! Use the echodataflow_start function, which is a central piece of Echodataflow, to kick off your pipeline. Import this function from Echodataflow and provide the paths or URLs of the configuration files. You can also pass additional options or storage options as needed. Here's an example:

Customize the paths, block name, storage type, and options based on your requirements.


```python
from echodataflow import echodataflow_start, StorageType, load_block

dataset_config = # url or path of datastore.yaml
pipeline_config = # url or path of pipeline.yaml
logfile_config = # url or path of logging.yaml (Optional)

aws = load_block(name="<block_name>", type=<StorageType>)

options = {"storage_options_override": False} # Enabling this assigns the block for universal use, avoiding the need for repetitive configurations when employing a single credential block throughout the application.
data  = echodataflow_start(dataset_config=dataset_config, pipeline_config=pipeline_config, logging_config=logfile_config, storage_options=aws, options=options)
```

## License

Licensed under the MIT License; you may not use this file except in compliance with the License. You may obtain a copy of the License [here](./LICENSE).
