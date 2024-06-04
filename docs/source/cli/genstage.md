# Code Generation

Echodataflow packages function calls to provide flexibility in configuring and running stages, as well as managing their attributes. This encapsulation allows for the execution and deployment of individual flows on any infrastructure. To facilitate this, Echodataflow provides a CLI command that generates the boilerplate code needed to create new stages. Simply add the necessary function calls, and your new stage is ready to go. Remember to add the new stage to the [rules](./rules) configuration.

## Subcommand

### `echodataflow gs`

**Usage**: To generate boilerplate code for a specific stage

```sh
echodataflow gs <stage_name>
```

**Arguments**:

<stage_name>: Name of the stage for which to generate boilerplate code.

**Example**

```sh
echodataflow gs compute_Sv
```

This command creates a template configuration file for the specified stage, allowing you to customize and integrate it into your workflow. The generated file includes:

- A `flow`: this orchestrates the execution of all files that need to be processed, either concurrently or in parallel, based on the configuration.
- A `task` (helper function): this assists the flow by processing individual files.