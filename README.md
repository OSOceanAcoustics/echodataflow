# Echodataflow

Echodataflow provides recipe-driven orchestration for fisheries acoustics workflows. It
combines [Prefect](https://www.prefect.io/), YAML deployment recipes, and processing tools
such as [Echopype](https://github.com/echostack-org/echopype) to run repeatable workflows on
edge, local, and cloud infrastructure.

Echodataflow `0.1.x` is deprecated. The repository currently contains the architecture being
prepared for the `0.2` release.

## Installation

```shell
conda create -n echodataflow -c conda-forge python=3.12 uv
conda activate echodataflow
uv pip install "git+https://github.com/echostack-org/echodataflow.git"
```

Install Echopype separately when using the acoustic processing flows:

```shell
uv pip install echopype
```

## Documentation

The documentation covers:

- installation and architecture;
- parameter and deployment recipes;
- Prefect server and worker setup on macOS and Linux;
- workflow deployment and troubleshooting;
- a reproducible simulated edge example; and
- development setup and adding operations, tasks, and flows.

Read the published documentation at
[echodataflow.readthedocs.io](https://echodataflow.readthedocs.io/) or build it locally:

```shell
uv pip install -e ".[docs]"
cd docs
jupyter book start
```

Mission-specific recipes are maintained in
[echodataflow-recipes](https://github.com/echostack-org/echodataflow-recipes).

## Development

```shell
git clone https://github.com/echostack-org/echodataflow.git
cd echodataflow
conda create -n echodataflow-dev -c conda-forge python=3.12 uv
conda activate echodataflow-dev
uv pip install -e ".[test,lint,docs]"
pytest
```

See the development guide in the documentation for project structure and contribution
instructions.

## License

Echodataflow is distributed under the [Apache License 2.0](LICENSE).
