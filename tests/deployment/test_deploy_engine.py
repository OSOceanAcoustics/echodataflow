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


def test_filter_flows_for_deploy_uses_flow_alias_fallback(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    all_flows = {
        "copy_raw": {
            "flow_obj": object(),
            "flow_module": "flows_helper",
            "flow_function_name": "flow_copy_raw",
        },
        "file_upload": {
            "flow_obj": object(),
            "flow_module": "flows_helper",
            "flow_function_name": "flow_file_upload",
        },
    }
    deploy_cfg = {
        "flows": {
            "copy_raw": {"module": "flows_helper"},
            "file_upload_acoustics": {
                "module": "flows_helper",
                "flow_alias": "file_upload",
            },
        }
    }

    filtered = engine.filter_flows_for_deploy(all_flows, deploy_cfg)

    assert set(filtered) == {"copy_raw", "file_upload_acoustics"}
    assert filtered["copy_raw"] is all_flows["copy_raw"]
    assert filtered["file_upload_acoustics"] is all_flows["file_upload"]


def test_filter_flows_for_deploy_raises_when_key_and_alias_missing(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    all_flows = {
        "copy_raw": {
            "flow_obj": object(),
            "flow_module": "flows_helper",
            "flow_function_name": "flow_copy_raw",
        }
    }
    deploy_cfg = {
        "flows": {
            "file_upload_acoustics": {
                "module": "flows_helper",
                "flow_alias": "file_upload",
            }
        }
    }

    with pytest.raises(KeyError, match="file_upload_acoustics"):
        engine.filter_flows_for_deploy(all_flows, deploy_cfg)


def test_local_deploy_specs_generate_current_flow_targets(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_ship = {
        "flows": {
            "copy_raw": {"module": "flows_helper", "interval": 1},
            "raw2Sv": {"module": "flows_acoustics", "interval": 1},
            "create_MVBS": {"module": "flows_acoustics", "interval": 1},
            "predict_hake": {"module": "flows_acoustics", "interval": 1},
            "file_upload_acoustics": {
                "module": "flows_helper",
                "flow_alias": "file_upload",
                "interval": 1,
            },
            "file_upload_trawl": {
                "module": "flows_helper",
                "flow_alias": "file_upload",
                "interval": 1,
            },
        }
    }
    deploy_cloud = {
        "flows": {
            "ingest_haul": {"module": "flows_biology", "interval": 1},
            "ingest_NASC": {"module": "flows_integration", "interval": 1},
            "update_grid": {"module": "flows_integration", "interval": 1},
            "update_cache_MVBS": {"module": "flows_viz_cloud", "interval": 1},
        }
    }
    param_ship = {"flows": {flow_key: {} for flow_key in deploy_ship["flows"]}}
    param_cloud = {"flows": {flow_key: {} for flow_key in deploy_cloud["flows"]}}

    # Build filtered flows mappings with mock flow objects
    ship_flows = {}
    for flow_key, flow_meta in deploy_ship["flows"].items():
        module_name = flow_meta["module"]
        flow_alias = flow_meta.get("flow_alias") or flow_key
        ship_flows[flow_key] = {
            "flow_obj": object(),
            "flow_module": module_name,
            "flow_function_name": f"flow_{flow_alias}",
        }

    cloud_flows = {}
    for flow_key, flow_meta in deploy_cloud["flows"].items():
        module_name = flow_meta["module"]
        flow_alias = flow_meta.get("flow_alias") or flow_key
        cloud_flows[flow_key] = {
            "flow_obj": object(),
            "flow_module": module_name,
            "flow_function_name": f"flow_{flow_alias}",
        }

    ship_specs = engine.build_deploy_specs(
        param_cfg=param_ship,
        deploy_cfg=deploy_ship,
        filtered_flows=ship_flows,
    )
    cloud_specs = engine.build_deploy_specs(
        param_cfg=param_cloud,
        deploy_cfg=deploy_cloud,
        filtered_flows=cloud_flows,
    )

    ship_targets = {spec.flow_key: spec.entrypoint for spec in ship_specs}
    cloud_targets = {spec.flow_key: spec.entrypoint for spec in cloud_specs}

    assert ship_targets == {
        "copy_raw": "echodataflow/flows/flows_helper.py:flow_copy_raw",
        "raw2Sv": "echodataflow/flows/flows_acoustics.py:flow_raw2Sv",
        "create_MVBS": "echodataflow/flows/flows_acoustics.py:flow_create_MVBS",
        "predict_hake": "echodataflow/flows/flows_acoustics.py:flow_predict_hake",
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
        filtered_flows={
            "emit_event_ABC": {
                "flow_obj": object(),
                "flow_module": "flows_test",
                "flow_function_name": "flow_emit_event_ABC",
            }
        },
    )

    assert specs[0].parameters == {"msg": "hello"}


def test_build_deploy_specs_rejects_entrypoint_override(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_cfg = {
        "flows": {
            "ingest_NASC": {
                "deployment_name": "ingest_NASC",
                "entrypoint": "echodataflow/flows/flows_integration.py:flow_ingest_NASC",
            }
        }
    }
    filtered_flows = {
        "ingest_NASC": {
            "flow_obj": object(),
            "flow_module": "flows_integration",
            "flow_function_name": "flow_ingest_NASC",
        }
    }
    param_cfg = {"flows": {"ingest_NASC": {}}}

    with pytest.raises(ValueError, match="entrypoint is not supported"):
        engine.build_deploy_specs(
            param_cfg=param_cfg,
            deploy_cfg=deploy_cfg,
            filtered_flows=filtered_flows,
        )


def test_build_deploy_specs_rejects_triggers_and_interval(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_cfg = {
        "flows": {
            "ingest_NASC": {
                "deployment_name": "ingest_NASC",
                "interval": 5,
                "triggers": [
                    {"expect": "nasc.ingested", "resource_name": "ingest_NASC"}
                ],
            }
        }
    }
    filtered_flows = {
        "ingest_NASC": {
            "flow_obj": object(),
            "flow_module": "flows_integration",
            "flow_function_name": "flow_ingest_NASC",
        }
    }
    param_cfg = {"flows": {"ingest_NASC": {}}}

    with pytest.raises(ValueError, match="exactly one of 'triggers' or 'interval'"):
        engine.build_deploy_specs(
            param_cfg=param_cfg,
            deploy_cfg=deploy_cfg,
            filtered_flows=filtered_flows,
        )


def test_build_deploy_specs_rejects_missing_triggers_and_interval(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_cfg = {
        "flows": {
            "ingest_NASC": {
                "deployment_name": "ingest_NASC",
            }
        }
    }
    filtered_flows = {
        "ingest_NASC": {
            "flow_obj": object(),
            "flow_module": "flows_integration",
            "flow_function_name": "flow_ingest_NASC",
        }
    }
    param_cfg = {"flows": {"ingest_NASC": {}}}

    with pytest.raises(ValueError, match="exactly one of 'triggers' or 'interval'"):
        engine.build_deploy_specs(
            param_cfg=param_cfg,
            deploy_cfg=deploy_cfg,
            filtered_flows=filtered_flows,
        )


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
    filtered_flows = {
        "ingest_NASC": {
            "flow_obj": object(),
            "flow_module": "flows_integration",
            "flow_function_name": "flow_ingest_NASC",
        }
    }
    param_cfg = {"flows": {"ingest_NASC": {}}}

    with pytest.raises(ValueError, match="must contain at least one trigger"):
        engine.build_deploy_specs(
            param_cfg=param_cfg,
            deploy_cfg=deploy_cfg,
            filtered_flows=filtered_flows,
        )


def test_build_deploy_specs_rejects_trigger_missing_resource_name(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    deploy_cfg = {
        "flows": {
            "ingest_NASC": {
                "deployment_name": "ingest_NASC",
                "triggers": [
                    {"expect": "nasc.ingested"}
                ],
            }
        }
    }
    filtered_flows = {
        "ingest_NASC": {
            "flow_obj": object(),
            "flow_module": "flows_integration",
            "flow_function_name": "flow_ingest_NASC",
        }
    }
    param_cfg = {"flows": {"ingest_NASC": {}}}

    with pytest.raises(ValueError, match="non-empty 'resource_name'"):
        engine.build_deploy_specs(
            param_cfg=param_cfg,
            deploy_cfg=deploy_cfg,
            filtered_flows=filtered_flows,
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
    filtered_flows = {
        "create_MVBS": {
            "flow_obj": _FakeFlow(),
            "flow_module": "flows_acoustics",
            "flow_function_name": "flow_create_MVBS",
        }
    }

    with pytest.raises(ValueError, match="does not define 'time_offset_seconds'"):
        engine.build_deploy_specs(
            param_cfg=param_cfg,
            deploy_cfg=deploy_cfg,
            filtered_flows=filtered_flows,
        )
