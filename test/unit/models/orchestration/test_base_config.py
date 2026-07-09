from etl_pipeline.models.orchestration.base_config import BaseConfig, Metadata


class TestBaseConfig:
    """Test class specifically for BaseConfig"""
    
    def test_config_with_valid_data(self):
        """Test config with valid data"""
        config = BaseConfig(
            kind="test_kind",
            metadata=Metadata(name="test_metadata")
        )
        assert config is not None
        assert config.kind == "test_kind"
        assert config.metadata.name == "test_metadata"
        
    def test_config_initialization_with_different_data(self):
        """
            Test config initialization with different data
        """
        test_cases = [
            {"kind": "test_kind", "metadata": Metadata(name="test_metadata")},
            {"kind": "test_kind2", "metadata": Metadata(name="test_metadata2")},
        ]

        for test_case in test_cases:
            config = BaseConfig(**test_case)
            assert config.kind == test_case["kind"]
            assert config.metadata.name == test_case["metadata"].name  

    def test_config_repr(self):
        """Test string representation"""
        config = BaseConfig(kind="test_kind", metadata=Metadata(name="test_metadata"))
        repr_str = repr(config)

        assert "BaseConfig" in repr_str
        assert "kind=" in repr_str
        assert "metadata=" in repr_str

    def test_config_equality(self):
        """
            Test equality check between instances
        """
        config1 = BaseConfig(kind="test_kind", metadata=Metadata(name="test_metadata"))
        config2 = BaseConfig(kind="test_kind", metadata=Metadata(name="test_metadata"))
        config3 = BaseConfig(kind="different", metadata=Metadata(name="different"))
        
        assert config1 == config2
        assert config1 != config3
