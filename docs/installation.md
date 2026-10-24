# Installation

Echodataflow is available and tested for Python 3.13-3.14. 
We recommend using a Conda environment because of its scientific and geospatial dependencies.


## Create a Conda environment

Create and activate an environment:

```shell
conda create --name echodataflow -c conda-forge python=3.13 uv
conda activate echodataflow
```

If your system does not already have rclone, include it when creating 
the environment by using the command below, then activate the environment:

```shell
conda create --name echodataflow -c conda-forge python=3.13 uv rclone
conda activate echodataflow
```

Once done, verify that rclone is installed by running:
```shell
rclone version
```


## Install from the repository

The Echodataflow `v0.2.0` release is currently under development.

To run Echodataflow as an installed package, install it directly from the repository:
```shell
uv pip install "echodataflow[dev,mission,inference] @ git+https://github.com/echostack-org/echodataflow.git"
```

To install Echodataflow for development, clone the repository and install from 
the local repository where you will be making code changes:
```shell
git clone https://github.com/echostack-org/echodataflow.git  # clone the repo
cd echodataflow  # switch into the repo directory
uv pip install -e ".[dev,mission,inference]"  # install in editable mode from the local repo
```

The installation enables the `echodataflow-deploy` command, which can be run from any directory.

Verify that the package is correctly installed by running:
```shell
python -c "import echodataflow; print(echodataflow.__version__)"
```

:::{note}
Adding `[dev,mission,inference]` in the `uv pip install` command above
install the full suite of dependencies specified in `pyproject.toml`:

- `dev`: testing, linting, code quality, documentation tools, and Jupyter kernel support.
- `mission`: mission-specific dependencies for the example pipelines.
- `inference`: the hake segmentation model, needed for running the `classify_backscatter` flow.
:::

:::{note}
The hake segmentation model weight can be separately downloaded from [LINK] and
set its path via the `path_weight` for the `classify_backscatter` flow to function.
:::



## Get the deployment recipes

Example recipes are maintained separately on a companion repository
[echodataflow-recipes](https://github.com/echostack-org/echodataflow-recipes).
You can download the recipes directly from the repo or get a copy by cloning it:

```shell
git clone https://github.com/echostack-org/echodataflow-recipes.git
```
