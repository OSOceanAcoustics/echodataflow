import importlib
import types
from pathlib import Path


def test_validate_flow_coverage(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    param_cfg = {"flows": {"flow_a": {}, "flow_b": {}}}
    deploy_cfg = {"flows": {"flow_a": {}, "flow_b": {}}}

    # Exact match — should not raise
    engine.validate_flow_coverage(param_cfg, deploy_cfg)

    # flow_b missing from deploy — should raise
    deploy_cfg_missing = {"flows": {"flow_a": {}}}
    import pytest
    with pytest.raises(ValueError, match="flow_b"):
        engine.validate_flow_coverage(param_cfg, deploy_cfg_missing)

    # flow_c in deploy but missing from config — should raise
    deploy_cfg_extra = {"flows": {"flow_a": {}, "flow_b": {}, "flow_c": {}}}
    with pytest.raises(ValueError, match="flow_c"):
        engine.validate_flow_coverage(param_cfg, deploy_cfg_extra)


def test_local_deploy_specs_generate_current_flow_entrypoints(install_prefect_stubs):
    install_prefect_stubs()
    engine = importlib.import_module("echodataflow.deployment.deployment_engine")

    repo_root = Path(__file__).resolve().parents[2]
    deploy_ship = engine.load_config(repo_root / "recipe" / "deploy" / "deploy_ship.yaml")
    deploy_cloud = engine.load_config(repo_root / "recipe" / "deploy" / "deploy_cloud.yaml")

    ship_registry = {
        module_name: types.ModuleType(module_name)
        for module_name in {flow_meta["module"] for flow_meta in deploy_ship["flows"].values()}
    }
    cloud_registry = {
        module_name: types.ModuleType(module_name)
        for module_name in {flow_meta["module"] for flow_meta in deploy_cloud["flows"].values()}
    }

    ship_specs = engine.build_specs_from_deploy_spec(
        deploy_cfg=deploy_ship,
        module_registry=ship_registry,
    )
    cloud_specs = engine.build_specs_from_deploy_spec(
        deploy_cfg=deploy_cloud,
        module_registry=cloud_registry,
    )

    ship_entrypoints = {spec.flow_key: spec.entrypoint for spec in ship_specs}
    cloud_entrypoints = {spec.flow_key: spec.entrypoint for spec in cloud_specs}

    assert ship_entrypoints == {
        "raw2Sv": "echodataflow/flows/flows_acoustics.py:flow_raw2Sv",
        "create_MVBS": "echodataflow/flows/flows_acoustics.py:flow_create_MVBS",
        "predict_hake": "echodataflow/flows/flows_acoustics.py:flow_predict_hake",
        "file_upload_acoustics": "echodataflow/flows/flows_helper.py:flow_file_upload",
        "file_upload_trawl": "echodataflow/flows/flows_helper.py:flow_file_upload",
    }
    assert cloud_entrypoints == {
        "ingest_haul": "echodataflow/flows/flows_biology.py:flow_ingest_haul",
        "ingest_NASC": "echodataflow/flows/flows_integration.py:flow_ingest_NASC",
        "update_grid": "echodataflow/flows/flows_integration.py:flow_update_grid",
        "update_cache_MVBS": "echodataflow/flows/flows_viz_cloud.py:flow_update_cache_MVBS",
    }
