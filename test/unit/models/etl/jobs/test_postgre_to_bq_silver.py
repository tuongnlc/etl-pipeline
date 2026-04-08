# test/unit/models/etl/jobs/test_postgre_to_bq_silver_config.py
from src.models.etl.jobs.postgre_to_bq_silver import PostgreToBqSilverConfig
from unittest.mock import Mock


class TestPostgreToBqSilverConfig:
    """Test class specifically for PostgreToBqSilverConfig"""
    
    def test_config_initialization(self):
        """
            Test config initialization with valid object types
        """
        # Mock objects to test
        mock_loader = Mock()
        mock_extractor = Mock()
        
        config = PostgreToBqSilverConfig(
            loader=mock_loader,
            extractor=mock_extractor
        )
        
        assert config.loader is mock_loader
        assert config.extractor is mock_extractor
    
    def test_config_with_different_object_types(self):
        """
            Test config initialization with different object types
        """
        test_cases = [
            # (loader, extractor)
            ("string_loader", "string_extractor"),
            (123, 456),
            ({"loader": "dict"}, {"extractor": "dict"}),
            (None, None),  
            ([1, 2, 3], {"key": "value"})
        ]
        
        for loader, extractor in test_cases:
            config = PostgreToBqSilverConfig(loader=loader, extractor=extractor)
            assert config.loader == loader
            assert config.extractor == extractor
    
    def test_config_representation(self):
        """Test string representation"""
        config = PostgreToBqSilverConfig(loader="test_loader", extractor="test_extractor")
        repr_str = repr(config)
        
        assert "PostgreToBqSilverConfig" in repr_str
        assert "loader=" in repr_str
        assert "extractor=" in repr_str
    
    def test_config_equality(self):
        """
            Test equality check between instances
        """
        config1 = PostgreToBqSilverConfig(loader="loader1", extractor="extractor1")
        config2 = PostgreToBqSilverConfig(loader="loader1", extractor="extractor1")
        config3 = PostgreToBqSilverConfig(loader="different", extractor="different")
        
        assert config1 == config2
        assert config1 != config3