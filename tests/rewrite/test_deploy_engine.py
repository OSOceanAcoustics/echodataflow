import importlib
import sys
import types


class FakeTrigger:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class FakeVariable:
    @classmethod
    def set(cls, key, value, overwrite):
        return None


def install_prefect_stubs(monkeypatch):
    prefect_mod = types.ModuleType("prefect")
    prefect_mod.deploy = lambda *args, **kwargs: None

    variables_mod = types.ModuleType("prefect.variables")
    variables_mod.Variable = FakeVariable

    events_mod = types.ModuleType("prefect.events")
    events_mod.DeploymentEventTrigger = FakeTrigger

    monkeypatch.setitem(sys.modules, "prefect", prefect_mod)
    monkeypatch.setitem(sys.modules, "prefect.variables", variables_mod)
    monkeypatch.setitem(sys.modules, "prefect.events", events_mod)


def test_validate_flow_coverage(monkeypatch):
    install_prefect_stubs(monkeypatch)
    engine = importlib.import_module("echodataflow.rewrite.deployment_engine")

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
