import importlib
import types
from pathlib import Path

import pytest


def test_validate_flow_coverage(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    param_cfg = {"flows": {"flow_a": {}, "flow_b": {}}}
    deploy_cfg = {"flows": {"flow_a": {}, "flow_b": {}}}

    # Exact match — should not raise
    engine.validate_flow_coverage(param_cfg, deploy_cfg)

    # flow_b missing from deploy — should raise
    deploy_cfg_missing = {"flows": {"flow_a": {}}}
    with pytest.raises(ValueError, match="flow_b"):
        engine.validate_flow_coverage(param_cfg, deploy_cfg_missing)

    # flow_c in deploy but missing from config — should raise
    deploy_cfg_extra = {"flows": {"flow_a": {}, "flow_b": {}, "flow_c": {}}}
    with pytest.raises(ValueError, match="flow_c"):
        engine.validate_flow_coverage(param_cfg, deploy_cfg_extra)


def test_local_deploy_specs_generate_current_flow_targets(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_ship = {
        "flows": {
            "copy_raw": {"interval": 1},
            "raw2Sv": {"interval": 1},
            "create_MVBS": {"interval": 1},
            "predict_hake": {"interval": 1},
            "file_upload_acoustics": {
                "flow": "file_upload",
                "interval": 1,
            },
            "file_upload_trawl": {
                "flow": "file_upload",
                "interval": 1,
            },
        }
    }
    deploy_cloud = {
        "flows": {
            "ingest_haul": {"interval": 1},
            "ingest_NASC": {"interval": 1},
            "update_grid": {"interval": 1},
            "update_cache_MVBS": {"interval": 1},
        }
    }
    param_ship = {"flows": {flow_key: {} for flow_key in deploy_ship["flows"]}}
    param_cloud = {"flows": {flow_key: {} for flow_key in deploy_cloud["flows"]}}

    # Build filtered flows mappings with mock flow objects
    ship_flows = {}
    ship_modules = {
        "copy_raw": "flows_simulation",
        "raw2Sv": "flows_acoustics",
        "create_MVBS": "flows_acoustics",
        "predict_hake": "flow_predict_hake",
        "file_upload_acoustics": "flows_helper",
        "file_upload_trawl": "flows_helper",
    }
    for flow_key, flow_meta in deploy_ship["flows"].items():
        module_name = ship_modules[flow_key]
        registry_key = flow_meta.get("flow") or flow_key
        ship_flows[flow_key] = {
            "flow_obj": object(),
            "entrypoint": (f"echodataflow/flows/{module_name}.py:flow_{registry_key}"),
        }

    cloud_flows = {}
    cloud_modules = {
        "ingest_haul": "flows_biology",
        "ingest_NASC": "flows_integration",
        "update_grid": "flows_integration",
        "update_cache_MVBS": "flows_viz_cloud",
    }
    for flow_key, flow_meta in deploy_cloud["flows"].items():
        module_name = cloud_modules[flow_key]
        cloud_flows[flow_key] = {
            "flow_obj": object(),
            "entrypoint": f"echodataflow/flows/{module_name}.py:flow_{flow_key}",
        }

    ship_specs = engine.build_deploy_specs(
        param_cfg=param_ship,
        deploy_cfg=deploy_ship,
        resolved_flows=ship_flows,
    )
    cloud_specs = engine.build_deploy_specs(
        param_cfg=param_cloud,
        deploy_cfg=deploy_cloud,
        resolved_flows=cloud_flows,
    )

    ship_targets = {spec.flow_key: spec.entrypoint for spec in ship_specs}
    cloud_targets = {spec.flow_key: spec.entrypoint for spec in cloud_specs}

    assert ship_targets == {
        "copy_raw": "echodataflow/flows/flows_simulation.py:flow_copy_raw",
        "raw2Sv": "echodataflow/flows/flows_acoustics.py:flow_raw2Sv",
        "create_MVBS": "echodataflow/flows/flows_acoustics.py:flow_create_MVBS",
        "predict_hake": "echodataflow/flows/flow_predict_hake.py:flow_predict_hake",
        "file_upload_acoustics": "echodataflow/flows/flows_helper.py:flow_file_upload",
        "file_upload_trawl": "echodataflow/flows/flows_helper.py:flow_file_upload",
    }
    assert cloud_targets == {
        "ingest_haul": "echodataflow/flows/flows_biology.py:flow_ingest_haul",
        "ingest_NASC": "echodataflow/flows/flows_integration.py:flow_ingest_NASC",
        "update_grid": "echodataflow/flows/flows_integration.py:flow_update_grid",
        "update_cache_MVBS": "echodataflow/flows/flows_viz_cloud.py:flow_update_cache_MVBS",
    }


def test_build_deploy_specs_passes_target_flow_parameters_directly(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    specs = engine.build_deploy_specs(
        param_cfg={"flows": {"emit_event_ABC": {"msg": "hello"}}},
        deploy_cfg={
            "flows": {
                "emit_event_ABC": {
                    "interval": 1,
                }
            }
        },
        resolved_flows={
            "emit_event_ABC": {
                "flow_obj": object(),
                "entrypoint": "echodataflow/flows/flows_test.py:flow_emit_event_ABC",
            }
        },
    )

    assert specs[0].parameters == {"msg": "hello"}


def test_validate_deploy_config_accepts_every_allowed_key(install_prefect_stubs):
    install_prefect_stubs()
    core = importlib.import_module("echodataflow.deployment.core")
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_cfg = {
        "flow_start_time": "2026-01-01T00:00:00+00:00",
        "default_work_pool_name": "default-pool",
        "source": {
            "mode": "git",
            "git": {
                "url": "https://example.com/repo.git",
                "branch": "main",
            },
        },
        "flows": {
            "scheduled": {
                "deployment_name": "scheduled-deployment",
                "flow": "actual_flow_name",
                "interval": 10,
                "cron_offset": 3,
                "inject_time_offset": True,
                "work_pool_name": "special-pool",
            },
            "event_driven": {
                "triggers": [
                    {
                        "expect": "prefect.flow-run.Completed",
                        "resource_name": "scheduled-deployment",
                    }
                ],
            },
        },
    }

    engine.validate_deploy_config(deploy_cfg)

    assert core.ALLOWED_DEPLOY_KEYS == {
        "flow_start_time",
        "default_work_pool_name",
        "source",
        "flows",
    }
    assert core.ALLOWED_FLOW_DEPLOY_KEYS == {
        "deployment_name",
        "flow",
        "interval",
        "cron_offset",
        "triggers",
        "inject_time_offset",
        "work_pool_name",
    }
    assert core.ALLOWED_TRIGGER_KEYS == {"expect", "resource_name"}
    assert core.ALLOWED_SOURCE_KEYS == {"mode", "git"}
    assert core.ALLOWED_GIT_SOURCE_KEYS == {"url", "branch"}


@pytest.mark.parametrize(
    ("deploy_cfg", "expected_path", "unknown_field"),
    [
        (
            {"flows": {}, "default_workpool_name": "local"},
            "deploy_cfg",
            "default_workpool_name",
        ),
        (
            {"flows": {"upstream": {"entrypoint": "flows.py:upstream"}}},
            "deploy_cfg.flows.upstream",
            "entrypoint",
        ),
        (
            {
                "flows": {
                    "downstream": {
                        "triggers": [
                            {
                                "expect": "prefect.flow-run.Completed",
                                "resource_name": "upstream",
                                "resource_role": "deployment",
                            }
                        ]
                    }
                }
            },
            "deploy_cfg.flows.downstream.triggers[0]",
            "resource_role",
        ),
        (
            {"flows": {}, "source": {"mode": "local", "directory": "/tmp"}},
            "deploy_cfg.source",
            "directory",
        ),
        (
            {
                "flows": {},
                "source": {
                    "mode": "git",
                    "git": {"url": "https://example.com/repo.git", "ref": "main"},
                },
            },
            "deploy_cfg.source.git",
            "ref",
        ),
    ],
)
def test_validate_deploy_config_rejects_unknown_nested_fields(
    install_prefect_stubs,
    deploy_cfg,
    expected_path,
    unknown_field,
):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    with pytest.raises(ValueError) as exc_info:
        engine.validate_deploy_config(deploy_cfg)

    message = str(exc_info.value)
    assert expected_path in message
    assert unknown_field in message


@pytest.mark.parametrize(
    ("deploy_cfg", "expected_message"),
    [
        (None, "deploy_cfg must be a mapping"),
        ({}, "deploy_cfg.flows must be a mapping"),
        ({"flows": []}, "deploy_cfg.flows must be a mapping"),
        (
            {"flows": {"raw2Sv": []}},
            "deploy_cfg.flows.raw2Sv must be a mapping",
        ),
        (
            {"flows": {"raw2Sv": {"flow": ""}}},
            "deploy_cfg.flows.raw2Sv.flow must be a non-empty string",
        ),
        (
            {"flows": {}, "source": "local"},
            "deploy_cfg.source must be a mapping",
        ),
        (
            {"flows": {}, "source": {"mode": "git", "git": "repo"}},
            "deploy_cfg.source.git must be a mapping",
        ),
    ],
)
def test_validate_deploy_config_rejects_invalid_mapping_shapes(
    install_prefect_stubs,
    deploy_cfg,
    expected_message,
):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    with pytest.raises(ValueError, match=expected_message):
        engine.validate_deploy_config(deploy_cfg)


def test_build_deploy_specs_rejects_triggers_and_interval(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_cfg = {
        "flows": {
            "ingest_NASC": {
                "deployment_name": "ingest_NASC",
                "interval": 5,
                "triggers": [{"expect": "nasc.ingested", "resource_name": "ingest_NASC"}],
            }
        }
    }
    resolved_flows = {
        "ingest_NASC": {
            "flow_obj": object(),
            "entrypoint": "echodataflow/flows/flows_integration.py:flow_ingest_NASC",
        }
    }
    param_cfg = {"flows": {"ingest_NASC": {}}}

    with pytest.raises(ValueError, match="only one of 'triggers' or 'interval'"):
        engine.build_deploy_specs(
            param_cfg=param_cfg,
            deploy_cfg=deploy_cfg,
            resolved_flows=resolved_flows,
        )


def test_build_deploy_specs_allows_manual_deployment_without_schedule(
    install_prefect_stubs,
):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_cfg = {
        "flows": {
            "ingest_NASC": {
                "deployment_name": "ingest_NASC",
            }
        }
    }
    resolved_flows = {
        "ingest_NASC": {
            "flow_obj": object(),
            "entrypoint": "echodataflow/flows/flows_integration.py:flow_ingest_NASC",
        }
    }
    param_cfg = {"flows": {"ingest_NASC": {}}}

    specs = engine.build_deploy_specs(
        param_cfg=param_cfg,
        deploy_cfg=deploy_cfg,
        resolved_flows=resolved_flows,
    )

    assert len(specs) == 1
    assert specs[0].cron is None
    assert specs[0].triggers is None


def test_build_deploy_specs_rejects_empty_triggers(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_cfg = {
        "flows": {
            "ingest_NASC": {
                "deployment_name": "ingest_NASC",
                "triggers": [],
            }
        }
    }
    resolved_flows = {
        "ingest_NASC": {
            "flow_obj": object(),
            "entrypoint": "echodataflow/flows/flows_integration.py:flow_ingest_NASC",
        }
    }
    param_cfg = {"flows": {"ingest_NASC": {}}}

    with pytest.raises(ValueError, match="must contain at least one trigger"):
        engine.build_deploy_specs(
            param_cfg=param_cfg,
            deploy_cfg=deploy_cfg,
            resolved_flows=resolved_flows,
        )


def test_build_deploy_specs_rejects_trigger_missing_resource_name(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_cfg = {
        "flows": {
            "ingest_NASC": {
                "deployment_name": "ingest_NASC",
                "triggers": [{"expect": "nasc.ingested"}],
            }
        }
    }
    resolved_flows = {
        "ingest_NASC": {
            "flow_obj": object(),
            "entrypoint": "echodataflow/flows/flows_integration.py:flow_ingest_NASC",
        }
    }
    param_cfg = {"flows": {"ingest_NASC": {}}}

    with pytest.raises(ValueError, match="non-empty 'resource_name'"):
        engine.build_deploy_specs(
            param_cfg=param_cfg,
            deploy_cfg=deploy_cfg,
            resolved_flows=resolved_flows,
        )


def test_build_deploy_specs_rejects_inject_time_offset_for_incompatible_flow(
    install_prefect_stubs,
):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    def _incompatible_flow_fn(path_main: str = ""):
        return None

    class _FakeFlow:
        fn = _incompatible_flow_fn

    deploy_cfg = {
        "flow_start_time": "2026-01-01T00:00:00+00:00",
        "flows": {
            "create_MVBS": {
                "deployment_name": "create_MVBS",
                "interval": 5,
                "inject_time_offset": True,
            }
        },
    }
    param_cfg = {"flows": {"create_MVBS": {"path_main": "/tmp"}}}
    resolved_flows = {
        "create_MVBS": {
            "flow_obj": _FakeFlow(),
            "entrypoint": "echodataflow/flows/flows_acoustics.py:flow_create_MVBS",
        }
    }

    with pytest.raises(ValueError, match="does not define 'time_offset_seconds'"):
        engine.build_deploy_specs(
            param_cfg=param_cfg,
            deploy_cfg=deploy_cfg,
            resolved_flows=resolved_flows,
        )
