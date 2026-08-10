DEFAULT_ENTRYPOINT_ROOT = "echodataflow/flows"

ALLOWED_DEPLOY_KEYS = {"flow_start_time", "default_work_pool_name", "source", "flows"}
ALLOWED_FLOW_DEPLOY_KEYS = {
    "deployment_name",
    "flow",
    "interval",
    "cron_offset",
    "triggers",
    "inject_time_offset",
    "work_pool_name",
}
ALLOWED_TRIGGER_KEYS = {"expect", "resource_name"}
ALLOWED_SOURCE_KEYS = {"mode", "git"}
ALLOWED_GIT_SOURCE_KEYS = {"url", "branch"}
