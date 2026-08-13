DEFAULT_ENTRYPOINT_ROOT = "echodataflow/flows"

ALLOWED_DEPLOY_KEYS = {
    "concurrency_groups",
    "flow_start_time",
    "default_work_pool_name",
    "source",
    "flows",
}
ALLOWED_FLOW_DEPLOY_KEYS = {
    "concurrency_group",
    "deployment_name",
    "flow",
    "interval",
    "cron_offset",
    "triggers",
    "inject_time_offset",
    "task_runner",
    "work_pool_name",
}
ALLOWED_CONCURRENCY_GROUP_KEYS = {"limit"}
ALLOWED_TASK_RUNNER_KEYS = {"type", "cluster_kwargs"}
ALLOWED_DASK_CLUSTER_KEYS = {
    "memory_limit",
    "n_workers",
    "processes",
    "threads_per_worker",
}
ALLOWED_TRIGGER_KEYS = {"expect", "resource_name"}
ALLOWED_SOURCE_KEYS = {"mode", "git"}
ALLOWED_GIT_SOURCE_KEYS = {"url", "branch"}
