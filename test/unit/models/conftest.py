# conftest.py
import pytest
from etl_pipeline.models.etl.jobs.postgre_to_bq_silver import PostgreToBqSilverConfig


@pytest.fixture(params=[
    (PostgreToBqSilverConfig, {"loader": "test_loader", "extractor": "test_extractor"}),
    # (OtherConfig, {"param1": "value1", "param2": "value2"})
])
def config_instance(request):
    config_class, kwargs = request.param
    return config_class(**kwargs)

def test_all_configs(config_instance):
    # Test common behavior for all configs
    assert config_instance is not None
    # Add more generic assertions