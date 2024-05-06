from pathlib import Path
from echodataflow.stages.echodataflow import echodataflow_start
import pytest

@pytest.fixture
def dataset_config():
    return Path("datastore.yaml").resolve()

@pytest.fixture(params=[Path("./MVBS_pipeline.yaml").resolve(), Path("./mask_pipeline.yaml").resolve()])
def pipeline_config(request):
    return request.param

def test_pipeline(dataset_config, pipeline_config):
    options = {"storage_options_override": False}
    data = echodataflow_start(dataset_config=dataset_config, pipeline_config=pipeline_config, options=options)
    assert data is not None, "Expected data to be non-None"
