import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import call

import pytest


def import_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_deploy_cli_module(install_prefect_stubs):
    install_prefect_stubs()
    module_path = Path(__file__).resolve().parents[2] / "src" / "echodataflow" / "deployment" / "deploy_cli.py"
    return import_module_from_path("deploy_cli_test_mod", module_path)


def test_build_parser_run_boolean_optional(install_prefect_stubs):
    module = _load_deploy_cli_module(install_prefect_stubs=install_prefect_stubs)

    parser = module._build_parser()
    args = parser.parse_args(
        [
            "run",
            "--default-work-pool-name",
            "local",
            "--param-config",
            "config_ship.yaml",
            "--deploy-spec",
            "deploy_ship.yaml",
            "--no-use-concurrency",
        ]
    )

    assert args.target == "run"
    assert args.use_concurrency is False
    assert args.source_mode is None
    assert args.param_config == Path("config_ship.yaml")
    assert args.deploy_spec == Path("deploy_ship.yaml")


def test_main_dispatches_run_args(monkeypatch, install_prefect_stubs):
    module = _load_deploy_cli_module(install_prefect_stubs=install_prefect_stubs)

    captured = {}

    def fake_run_from_specs(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(module, "_run_from_specs", fake_run_from_specs)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "deploy_cli",
            "run",
            "--default-work-pool-name",
            "local",
            "--param-config",
            "recipe/params/config_ship.yaml",
            "--deploy-spec",
            "recipe/deploy/deploy_ship.yaml",
            "--source-mode",
            "git",
            "--no-use-concurrency",
        ],
    )

    module.main()

    assert os.environ["PREFECT_SOURCE_MODE"] == "git"
    assert captured["param_cfg_path"] == Path("recipe/params/config_ship.yaml")
    assert captured["deploy_cfg_path"] == Path("recipe/deploy/deploy_ship.yaml")
    assert captured["module_prefix"] == "echodataflow.flows"
    assert captured["source_mode"] == "git"
    assert captured["run_concurrency_setup"] is False


def test_import_module_raises_when_prefixed_module_missing(
    monkeypatch, install_prefect_stubs
):
    module = _load_deploy_cli_module(install_prefect_stubs=install_prefect_stubs)
    prefixed = "echodataflow.flows.flows_acoustics"

    def fake_import(name):
        if name == prefixed:
            raise ModuleNotFoundError(f"No module named '{prefixed}'", name=prefixed)
        raise AssertionError(f"unexpected import: {name}")

    import importlib as _importlib
    from unittest.mock import Mock

    mock_import = Mock(side_effect=fake_import)
    monkeypatch.setattr(_importlib, "import_module", mock_import)

    with pytest.raises(ModuleNotFoundError, match=prefixed):
        module._import_module("flows_acoustics", "echodataflow.flows")

    assert mock_import.mock_calls == [call(prefixed)]


def test_import_module_raises_nested_dependency_error(monkeypatch, install_prefect_stubs):
    module = _load_deploy_cli_module(install_prefect_stubs=install_prefect_stubs)

    def fake_import(name):
        if name == "echodataflow.flows.flows_biology":
            raise ModuleNotFoundError("No module named 'pandas'", name="pandas")
        raise AssertionError(f"unexpected import: {name}")

    import importlib as _importlib
    from unittest.mock import Mock

    mock_import = Mock(side_effect=fake_import)
    monkeypatch.setattr(_importlib, "import_module", mock_import)

    with pytest.raises(ModuleNotFoundError, match="pandas"):
        module._import_module("flows_biology", "echodataflow.flows")

    assert mock_import.mock_calls == [call("echodataflow.flows.flows_biology")]
