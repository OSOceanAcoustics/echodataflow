# echoflow

**NOTE: This project is currently under heavy development,
and has not been tested or deployed on the cloud**

Sonar conversion pipeline tool with echopype.
This tool allows for users to quickly setup sonar data processing pipelines,
and deploy it locally or on the cloud. It uses [Prefect 2.0](https://www.prefect.io/),
a data workflow orchestration tool to run these flows on various platforms.

## Development

To develop the code, simply install the package in a python virtual environment in editable mode.

```bash
pip install -e .[all]
```

This will install all of the dependencies that the package need.

**Check out the [Hake Flow Demo](./notebooks/HakeFlowDemo.ipynb) notebook to get started.**

## Package structure

All of the code lives in a directory called [echoflow](./echoflow/).

Under that directory, there are currently 4 main subdirectory:

- [settings](./echoflow/settings/): This is where pipeline configurations object models are found,
as well as a home for any package configurations.
  - [models](./echoflow/settings/models/): This sub-directory to `settings` contains [pydantic](https://docs.pydantic.dev/) models to validate the configuration file specified by the user.
  This can look like below in [YAML](https://yaml.org/) format.

    ```yaml
    name: Bell_M._Shimada-SH1707-EK60
    sonar_model: EK60
    raw_regex: (.*)-?D(?P<date>\w{1,8})-T(?P<time>\w{1,6})
    args:
      urlpath: s3://ncei-wcsd-archive/data/raw/{{ ship_name }}/{{ survey_name }}/{{ sonar_model }}/*.raw
      # Set default parameter values as found in urlpath
      parameters:
        ship_name: Bell_M._Shimada
        survey_name: SH1707
        sonar_model: EK60
      storage_options:
        anon: true
    transect:
        # Transect file spec
        # can be either single or multiple files
        file: ./hake_transects_2017.zip
    output:
      urlpath: ./combined_files
      overwrite: true
    ```

    This yaml file turns into a `MainConfig` object that looks like:

    ```python
    MainConfig(name='Bell_M._Shimada-SH1707-EK60', sonar_model='EK60', raw_regex='(.*)-?D(?P<date>\\w{1,8})-T(?P<time>\\w{1,6})', args=Args(urlpath='s3://ncei-wcsd-archive/data/raw/{{ ship_name }}/{{ survey_name }}/{{ sonar_model }}/*.raw', parameters={'ship_name': 'Bell_M._Shimada', 'survey_name': 'SH1707', 'sonar_model': 'EK60'}, storage_options={'anon': True}, transect=Transect(file='./hake_transects_2017.zip', storage_options={})), output=Output(urlpath='./combined_files', storage_options={}, overwrite=True), echopype=None)
    ```

- [stages](./echoflow/stages/): Within this directory lives the code for various stages within the sonar data processing pipeline, which is currently sketched out and discussed [here](https://github.com/uw-echospace/data-processing-levels/blob/main/discussion_2022-07-12.md).
- [subflows](./echoflow/subflows/): Subflows contains flows that support processing level flows.
Essentially this is the individual smaller flows that need to run within a data processing level.
  - Currently, each subflow is a directory that contains the following python files:
    - `flows.py`: Code regarding to flow lives here
    - `tasks.py`: Code regarding to task lives here
    - `utils.py`: Code that is used for utility functions lives here
    - `__init__.py`: File to import flow so the subflow directory can become a module and flow to be easily imported.
- [tests](./echoflow/tests/): Tests code lives in this directory.

For more details about prefect, go to their extensive [documentation](https://docs.prefect.io/).

## License

Licensed under the MIT License; you may not use this file except in compliance with the License. You may obtain a copy of the License [here](./LICENSE).
