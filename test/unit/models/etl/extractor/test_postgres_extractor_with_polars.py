# test/unit/models/etl/extractor/test_postgres_extractor_config.py
import pytest
from src.models.etl.extractor.postgres_extractor_with_polars import PostgreDBExtractorWithPolarsConfig


class TestPostgreDBExtractorWithPolarsConfig:
    """Test class for PostgreDBExtractorWithPolarsConfig"""
    
    def test_config_initialization_with_valid_data(self):
        """
            Test with valid data
        """
        config = PostgreDBExtractorWithPolarsConfig(
            uri="postgresql://user:pass@localhost:5432/mydb",
            query="SELECT * FROM table"
        )
        
        assert config.uri == "postgresql://user:pass@localhost:5432/mydb"
        assert config.query == "SELECT * FROM table"
    
    def test_config_initialization_with_different_data(self):
        """
            Test with different data
        """
        test_cases = [
            {
                "uri": "postgresql://test@server/db",
                "query": "SELECT col1, col2 FROM table WHERE condition = true"
            },
            {
                "uri": "postgresql://user2:pass2@prod-server:5432/production",
                "query": "SELECT COUNT(*) FROM large_table"
            }
        ]
        
        for test_case in test_cases:
            config = PostgreDBExtractorWithPolarsConfig(**test_case)
            assert config.uri == test_case["uri"]
            assert config.query == test_case["query"]
    
    def test_config_repr(self):
        """
            Test repr string
        """
        config = PostgreDBExtractorWithPolarsConfig(
            uri="postgresql://user@localhost/db",
            query="SELECT * FROM table"
        )
        
        repr_str = repr(config)
        assert "PostgreDBExtractorWithPolarsConfig" in repr_str
        assert "uri=" in repr_str
        assert "query=" in repr_str
    
    def test_config_equality(self):
        """
            Test equality check between instances
        """
        config1 = PostgreDBExtractorWithPolarsConfig(
            uri="postgresql://user@localhost/db",
            query="SELECT * FROM table"
        )
        
        config2 = PostgreDBExtractorWithPolarsConfig(
            uri="postgresql://user@localhost/db", 
            query="SELECT * FROM table"
        )
        
        config3 = PostgreDBExtractorWithPolarsConfig(
            uri="postgresql://different@server/db",
            query="SELECT * FROM other_table"
        )
        
        assert config1 == config2  # Same values
        assert config1 != config3  # Different values