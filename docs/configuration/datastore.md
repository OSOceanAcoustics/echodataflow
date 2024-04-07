# Echoflow Run Configuration Documentation

This document provides detailed explanations for the keys used in the provided YAML configuration used to define an Echoflow run.

## Run Details

- `name`: This key specifies the name of the Echoflow run. It is used to identify and label the execution of the Echoflow process.
- `sonar_model`: This key indicates the model of the sonar used for data collection during the run.
- `raw_regex`: This key indicates the regex to be used while parsing the source directory to match the files to be processed.

## Input Arguments

- `urlpath`: This key defines the source data URL pattern for accessing raw data files. The pattern can contain placeholders that will be dynamically replaced during execution.
- `parameters`: This section holds parameters used in the source data URL. These parameters dynamically replace placeholders in the URL path.
- `storage_options`: This section defines storage options for accessing source data. It may include settings to anonymize access to the data.
- `transect`: This section provides information about the transect data, including the URL of the transect file and storage options.
- `json_export`: When set to true, this key indicates that raw JSON metadata of files should be exported for processing.
- `raw_json_path`: This key defines the path where the raw JSON metadata will be stored. It can be used to skip parsing files in the source directory and instead fetch files from this JSON.

## Output Arguments

- `urlpath`: This key defines the destination data URL where processed data will be stored.
- `overwrite`: When set to true, this key specifies that the data should overwrite any existing data in the output directory.
- `storage_options`: This section defines storage options for the destination data, which may include details such as the block name and type.

## Notes

- The provided configuration serves as a structured setup for executing an Echoflow run, allowing customization through the specified keys.
- Dynamic placeholders like `ship_name`, `survey_name`, and `sonar_model` are replaced with actual values based on the context.

Example:

```yaml
name: Bell_M._Shimada-SH1707-EK60 # Name of the Echoflow Run
sonar_model: EK60 # Sonar Model
raw_regex: (.*)-?D(?P<date>\w{1,8})-T(?P<time>\w{1,6}) # Regex to parse the filenames
args: # Input arguments
  urlpath: s3://ncei-wcsd-archive/data/raw/{{ ship_name }}/{{ survey_name }}/{{ sonar_model }}/*.raw # Source data URL
  parameters: # Source data URL parameters
    ship_name: Bell_M._Shimada
    survey_name: SH1707
    sonar_model: EK60
  storage_options: # Source data storage options
    anon: true
  group: # Source data transect information
    file: ./x0007_fileset.txt       # Transect file URL. Accepts .zip or .txt file
    storage_options: # Transect file storage options
      block_name: echoflow-aws-credentials # Block name. For more information on Blocks refer blocks.md
      type: AWS # Block type 
    group_name: default_group # Set when not using a file to pass transect information
  json_export: true # Export raw json metadata of files to be processed
  raw_json_path: s3://echoflow-workground/combined_files/raw_json # Path to store the raw json metadata. Can also work to skip the process of parsing the files at source directory and fetch files present in this json instead.
output: # Output arguments
  urlpath: s3://echoflow-workground/combined_files_dask # Destination data URL parameters
  overwrite: true # Flag to overwrite the data if present in the output directory
  storage_options: # Destination data storage options
    block_name: echoflow-aws-credentials
    type: AWS
```
