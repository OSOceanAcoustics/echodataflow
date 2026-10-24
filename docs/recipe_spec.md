(recipe-spec)=
# Recipe specifications


## Recipes are a pair

A parameter recipe and deployment recipe are validated together. If one declares
`file_upload_acoustics`, the other must declare the same key:

```yaml
# params_edge.yaml
flows:
  file_upload_acoustics:
    src_dir: /data/processed
    dest_dir: cloud:bucket/mission/acoustics
    exclude_subdirs: [Sv]
    max_age: 2
```

```yaml
# deploy_edge.yaml
default_work_pool_name: local
flows:
  file_upload_acoustics:
    flow: file_upload
    deployment_name: file-upload-acoustics
    interval: 10
```

The recipe key identifies this particular deployment. The optional `flow` field points it to
a registered implementation. This is how multiple deployments can reuse `file_upload` with
different source directories or destinations.

## Schedules and triggers

An `interval` is expressed in minutes and compiled to a cron expression. `cron_offset`
delays the first minute within the hour, which is useful when one flow should normally follow
another. For example, an interval of 10 with an offset of 3 runs at minutes 3, 13, 23, and so
on.

A flow may instead define `triggers`. An event-triggered flow starts when Prefect observes
the configured event from a related deployment. A single flow entry cannot use both an
interval and triggers.

Schedule offsets do not establish a data dependency. Flows should remain safe when an
upstream run is slow, empty, or unsuccessful.

## Historical simulation

Some flows calculate a processing window relative to the current time. A deployment recipe
can set `flow_start_time` and enable `inject_time_offset` for flows that accept
`time_offset_seconds`. This maps the current wall clock to a historical mission clock and is
used by the simulation examples.

Only enable injection for flows that declare `time_offset_seconds`; deployment validation
rejects incompatible flows.





This is a curated reference for operators and workflow developers. Echodataflow uses Jupyter
Book 2, which does not yet provide Sphinx-style Python autodoc, so source docstrings remain
the detailed reference for internal helpers.

## Deployment command

```text
echodataflow-deploy run
  --default-work-pool-name POOL
  --param-config PARAMS.yaml
  --deploy-spec DEPLOY.yaml
```

All three options are required. The pool supplied on the command line is used unless the
deployment recipe defines `default_work_pool_name`; a flow entry can override that default
with `work_pool_name`.

## Deployment recipe fields

Top-level fields:

| Field | Required | Meaning |
| --- | --- | --- |
| `flows` | yes | Mapping of recipe keys to deployment settings. |
| `flow_start_time` | no | ISO 8601 historical start used to calculate replay offsets. |
| `default_work_pool_name` | no | Default pool for flows in this recipe. |
| `source` | no | Source selection; defaults to local. |

`source` accepts `mode: local` or `mode: git`. Git mode requires `git.url` and optionally
accepts `git.branch`, which defaults to `main`.

Each entry below `flows` accepts:

| Field | Meaning |
| --- | --- |
| `deployment_name` | Name shown by Prefect; defaults to the recipe key. |
| `flow` | Registry key when it differs from the recipe key. |
| `interval` | Cron-like repetition interval in minutes. |
| `cron_offset` | Starting minute offset applied to an interval schedule. |
| `triggers` | Event triggers; mutually exclusive with `interval`. |
| `inject_time_offset` | Inject the historical offset into a compatible flow. |
| `work_pool_name` | Override the default work pool for this flow. |

Each trigger accepts `expect` and `resource_name`. Unknown fields are rejected rather than
silently ignored.

## Parameter recipe

The parameter recipe has one required top-level mapping:

```yaml
flows:
  registry-or-recipe-key:
    parameter_name: value
```

Values below a flow key are passed directly to its Prefect flow. Parameter and deployment
recipe keys must have one-to-one coverage.

## Registered flows

### `copy_raw`

Entrypoint: `echodataflow/flows/flows_simulation.py:flow_copy_raw`

Simulates realtime raw-file arrival by copying objects from a public S3 bucket when their
manifest timestamps fall between the previous and current simulated flow times.

| Parameter | Default | Purpose |
| --- | --- | --- |
| `time_offset_seconds` | `0.0` | Offset from wall-clock time for historical replay. |
| `path_raw_list` | empty | CSV containing `s3_path` and `timestamp`. |
| `path_copy` | empty | Local destination directory. |
| `s3_bucket` | `noaa-wcsd-pds` | Source bucket. |
| `exclude_before` | `None` | Ignore manifest timestamps at or before this time. |

The previous simulated time is stored in a deployment-specific Prefect Variable.

### `copy_trawl`

Entrypoint: `echodataflow/flows/flows_simulation.py:flow_copy_trawl`

Simulates sequential trawl-data arrival from an anonymously readable S3-compatible store.
Parameters select the destination, bucket, prefix, folder names, starting trawl number,
increment, and endpoint URL. The previous trawl number is stored per deployment.

### `raw2Sv`

Entrypoint: `echodataflow/flows/flows_acoustics.py:flow_raw2Sv`

Incrementally converts matching raw sonar files to Sv Zarr stores and updates an Sv CSV
index. Important parameters include `path_raw`, `path_main`, `sonar_model`,
`filename_pattern`, `waveform_mode`, `encode_mode`, `depth_offset`, `parallel`, and
`new_file_num_limit`.

The flow prevents concurrent runs of the same deployment. It requires Echopype and uses a
Dask task runner.

### `create_MVBS`

Entrypoint: `echodataflow/flows/flows_acoustics.py:flow_create_MVBS`

Creates MVBS products for a recent time window. `slice_mins` and `num_slices` determine the
window; `range_bin` and `ping_time_bin` determine binning. `path_main`, `file_Sv_csv`, and
`file_MVBS_csv` locate the product tree and indexes. This flow accepts historical time-offset
injection.

### `predict_hake`

Entrypoint: `echodataflow/flows/flow_predict_hake.py:flow_predict_hake`

Runs the hake segmentation model on recent MVBS products. It requires a compatible external
model package and weights. Parameters include the processing window, `temperature`,
`softmax_threshold`, `max_depth`, `path_weight`, and product index paths. This flow accepts
historical time-offset injection.

### `file_upload`

Entrypoint: `echodataflow/flows/flows_helper.py:flow_file_upload`

Copies files through rclone. `src_dir` and `dest_dir` use rclone path syntax;
`exclude_subdirs` omits directory trees and `max_age` limits uploads to files no older than
the specified hours. A value of `-1` disables the age limit. The flow creates and removes a
temporary exclusion file in the source directory.

### `ingest_haul`

Entrypoint: `echodataflow/flows/flows_biology.py:flow_ingest_haul`

Reads sets of length, specimen, catch, and net-configuration spreadsheets from cloud
storage, processes complete hauls, and updates combined biological tables and stratum means.
It requires a credential file understood by the flow's S3 filesystem configuration.

### `ingest_NASC`

Entrypoint: `echodataflow/flows/flows_integration.py:flow_ingest_NASC`

Reads cloud-hosted NASC Zarr products, incrementally combines them into a local table, and
associates results with survey grid information. Parameters control local/cloud paths,
credentials, filenames, reprocessing depth, and the maximum number of new files.

### `update_grid`

Entrypoint: `echodataflow/flows/flows_integration.py:flow_update_grid`

Updates gridded NASC results with the latest biological stratum means and calculates number
and biomass density. It operates on the local integration directory and configured grid
constants.

### `update_cache_MVBS`

Entrypoint: `echodataflow/flows/flows_viz_cloud.py:flow_update_cache_MVBS`

Builds a recent MVBS cache for visualization from cloud-hosted MVBS products. Parameters
select the time window, local cache, remote product location, credentials, and cache/index
filenames. This flow accepts historical time-offset injection.

## Operations and tasks

Operations currently cover raw-to-Sv conversion, MVBS creation, S3 simulation copies, hake
prediction, and NASC computation. Their associated task wrappers live under
`echodataflow.tasks`. These are implementation interfaces rather than recipe registry keys;
most users should deploy the flows above.

For adding or changing one of these layers, see [Development](development.md).

