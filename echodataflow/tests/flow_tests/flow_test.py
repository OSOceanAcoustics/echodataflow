from pathlib import Path
from echodataflow.stages.echodataflow import echodataflow_start
import pytest


ROOT_DIR = Path(__file__).resolve().parent.parent

@pytest.fixture
def dataset_config():
    return ROOT_DIR / "flow_tests/datastore.yaml"

@pytest.fixture(params=[
    ROOT_DIR / "flow_tests/MVBS_pipeline.yaml",
    ROOT_DIR / "flow_tests/mask_pipeline.yaml",
    ROOT_DIR / "flow_tests/TS_pipeline.yaml"
    ])
def pipeline_config(request):
    return request.param

def test_pipeline(dataset_config, pipeline_config):
    options = {"storage_options_override": False}
    data = echodataflow_start(dataset_config=dataset_config, pipeline_config=pipeline_config, options=options)
    assert data is not None, "Expected data to be non-None"
