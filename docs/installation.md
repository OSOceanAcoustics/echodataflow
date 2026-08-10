# Installation

Echodataflow requires Python 3.12 or newer. A Conda environment is recommended because
the processing stack contains compiled scientific and geospatial dependencies.

## Install from the source repository

The `0.2` release is still under development, so install the current source:

```shell
conda create -n echodataflow -c conda-forge python=3.12 uv
conda activate echodataflow
uv pip install "git+https://github.com/echostack-org/echodataflow.git"
```

This installs the `echodataflow-deploy` command.

```shell
echodataflow-deploy --help
python -c "import echodataflow; print(echodataflow.__version__)"
```

## Install the processing dependencies you need

Echodataflow orchestrates packages that are not all installed as core dependencies while
the `0.2` dependency set is being finalized. Install Echopype to run the acoustic conversion
flows:

```shell
uv pip install echopype
```

The `predict_hake` flow also needs the hake segmentation package and compatible model
weights:

```shell
git clone https://github.com/uw-echospace/segmentation_inference.git
uv pip install -e ./segmentation_inference
```

Model weights are not distributed with Echodataflow. Obtain the model approved for your
project and set `path_weight` in the parameter recipe.

## Install system tools

The file-upload flow invokes [rclone](https://rclone.org/). Install it with your platform's
package manager, then verify it is visible inside the environment used by the Prefect worker:

```shell
rclone version
```

For macOS background services, install `sqlite3` and Conda or Mamba at stable absolute
paths. On Linux, `systemd` must be available if you intend to use the service examples.

## Get the deployment recipes

Mission recipes are maintained separately from the package:

```shell
git clone https://github.com/echostack-org/echodataflow-recipes.git
```

Each deployment uses two YAML files:

- `recipes/params/params_*.yaml` contains arguments passed to flows.
- `recipes/deploy/deploy_*.yaml` contains schedules, triggers, work pools, and source code
  selection.

The paired files must contain the same keys below their top-level `flows` mappings.

## Connect to Prefect

For a local server:

```shell
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect server start
```

In another terminal, create a process work pool and start a worker:

```shell
prefect work-pool create --type process local
prefect worker start --pool local
```

If the pool already exists, Prefect reports that fact and it can be reused. See
[Deployment](deployment.md) before installing the server or worker as an operating-system
service.

## Install for development

Do not combine a development checkout with the commands above. Use the editable setup in
[Development](development.md), which includes tests, linting, and documentation tools.

