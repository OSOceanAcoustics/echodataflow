# Echodataflow

Echodataflow turns configuration files into repeatable fisheries acoustics workflows. It
combines [Prefect](https://www.prefect.io/) orchestration with processing functions from
[Echopype](https://echopype.readthedocs.io/) and other Echostack tools, allowing the same
workflow design to run on a shipboard computer, a workstation, or cloud infrastructure.

Echodataflow is currently under active development. The previous `0.1.x` design is
deprecated; these pages describe the forthcoming `0.2` architecture.

## Why Echodataflow?

Operational data pipelines must do more than call a sequence of functions. They need to
notice new files, avoid overlapping runs, retry transient failures, keep processing state,
move selected products between edge and cloud systems, and make failures observable.
Echodataflow provides those orchestration concerns around scientific processing code.

Its main goals are to:

- define deployments without embedding mission-specific paths and schedules in Python;
- reuse tested scientific operations across interactive, scheduled, and event-driven runs;
- support continuous processing where network access may be intermittent;
- make the movement from prototype to operational workflow incremental; and
- expose execution state, logs, retries, and schedules through Prefect.

## Where to begin

- [Installation](installation.md) sets up Echodataflow and its system tools.
- [How Echodataflow works](guide.md) introduces flows, tasks, operations, and recipes.
- [Deployment](deployment.md) covers Prefect, macOS `launchd`, Linux `systemd`, and
  deploying workflows.
- [Examples](examples.md) explains common recipe patterns and walks through a simulated
  edge workflow.
- [Development](development.md) covers a Conda and `uv` development environment and how
  to add a workflow step.
- [Workflow reference](reference.md) catalogs the currently registered flows and recipe
  fields.

## Project status

Recipes used for real missions live in the separate
[echodataflow-recipes](https://github.com/echostack-org/echodataflow-recipes) repository.
Treat those recipes as deployment-specific examples: review paths, credentials, schedules,
and resource requirements before using them on another system.

