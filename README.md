## Echoflow: Streamlined Data Pipeline Orchestration

Welcome to **Echoflow**! Echoflow is a powerful data pipeline orchestration tool designed to simplify and enhance the execution of data processing tasks. Leveraging the capabilities of [Prefect 2.0](https://www.prefect.io/) and YAML configuration files, Echoflow caters to the needs of scientific research and data analysis. It provides an efficient way to define, configure, and execute complex data processing workflows.

Echoflow integrates with **echopype**, a renowned package for sonar data analysis, to provide a versatile solution for researchers, analysts, and engineers. With Echoflow, users can seamlessly process and analyze sonar data using a modular and user-friendly approach.


# Getting Started with Echoflow

This guide will walk you through the initial steps to set up and run your Echoflow pipelines.

## 1. Create a Virtual Environment

To keep your Echoflow environment isolated, it's recommended to create a virtual environment using Conda or Python's built-in `venv` module. Here's an example using Conda:

```bash
conda create --name echoflow-env
conda activate echoflow-env
```

Or, using Python's venv:

```bash
python -m venv echoflow-env
source echoflow-env/bin/activate  # On Windows, use `echoflow-env\Scripts\activate`
```

## 2. Clone the Project
Now that you have a virtual environment set up, you can clone the Echoflow project repository to your local machine using the following command:

```bash
git clone <repository_url>
```

## 3. Install the Package
Navigate to the project directory you've just cloned and install the Echoflow package. The -e flag is crucial as it enables editable mode, which is especially helpful during development and testing.

```bash
cd <project_directory>
pip install -e .
```

## 4. Configure Blocks
Echoflow utilizes the concept of [blocks](./docs/configuration/blocks.md) which are secure containers for storing credentials and sensitive data. To set up your cloud credentials, configure blocks according to your cloud provider. For detailed instructions, refer to the [Blocks Configuration Guide](./docs/configuration/blocks.md#creating-credential-blocks).

## 5. Edit the Pipeline Configuration
Open the [pipeline.yaml](./docs/configuration/pipeline.md) file. This YAML configuration file defines the processes you want to execute as part of your pipeline. Customize it by adding the necessary stages and functions from echopype that you wish to run.

## 6. Define Data Sources and Destinations
Customize the [datastore.yaml](./docs/configuration/datastore.md) file to define the source and destination for your pipeline's data. This is where Echoflow will fetch and store data as it executes the pipeline.

## 7. Execute the Pipeline
You're now ready to execute your Echoflow pipeline! Use the echoflow_start function, which is a central piece of Echoflow, to kick off your pipeline. Import this function from Echoflow and provide the paths or URLs of the configuration files. You can also pass additional options or storage options as needed. Here's an example:

Customize the paths, block name, storage type, and options based on your requirements.


```python
from echoflow import echoflow_start, StorageType, load_block

dataset_config = # url or path of datastore.yaml
pipeline_config = # url or path of pipeline.yaml
logfile_config = # url or path of logging.yaml (Optional)

aws = load_block(name="<block_name>", type=<StorageType>)

options = {"storage_options_override": False} # Enabling this assigns the block for universal use, avoiding the need for repetitive configurations when employing a single credential block throughout the application.
data  = echoflow_start(dataset_config=dataset_config, pipeline_config=pipeline_config, logging_config=logfile_config, storage_options=aws, options=options)
```

## License

Licensed under the MIT License; you may not use this file except in compliance with the License. You may obtain a copy of the License [here](./LICENSE).
