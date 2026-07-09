from etl_pipeline.models.etl.loader.bq_loader_with_polars import BigQueryLoaderPolarsConfig


class TestBigQueryLoaderPolarsConfig:
    """Test class specifically for BigQueryLoaderPolarsConfig"""
    
    def test_config_with_valid_data(self):
        """Test config with valid data"""
        config = BigQueryLoaderPolarsConfig(
            gcp_credential_key="test_key",
            project="test_project",
            dataset="test_dataset",
            table="test_table"
        )

        assert config is not None
        assert config.gcp_credential_key == "test_key"
        assert config.project == "test_project"
        assert config.dataset == "test_dataset"
        assert config.table == "test_table"

    def test_config_initialization_with_different_data(self):
        """Test config initialization with different data"""
        test_cases = [
            {
                "gcp_credential_key": "test_key",
                "project": "test_project",
                "dataset": "test_dataset",
                "table": "test_table"
            },
            {
                "gcp_credential_key": "test_key2",
                "project": "test_project2",
                "dataset": "test_dataset2",
                "table": "test_table2"
            },
        ]

        for test_case in test_cases:
            config = BigQueryLoaderPolarsConfig(**test_case)
            assert config.gcp_credential_key == test_case["gcp_credential_key"]
            assert config.project == test_case["project"]
            assert config.dataset == test_case["dataset"]
            assert config.table == test_case["table"]

    def test_config_repr(self):
        """Test config representation"""
        config = BigQueryLoaderPolarsConfig(
            gcp_credential_key="test_key",
            project="test_project",
            dataset="test_dataset",
            table="test_table"
        )
        repr_str = repr(config)
        assert "BigQueryLoaderPolarsConfig" in repr_str
        assert "gcp_credential_key=" in repr_str
        assert "project=" in repr_str
        assert "dataset=" in repr_str
        assert "table=" in repr_str

    def test_config_equality(self):
        """
            Test equality check between instances
        """
        config1 = BigQueryLoaderPolarsConfig(
            gcp_credential_key="test_key",
            project="test_project",
            dataset="test_dataset",
            table="test_table"
        )
        config2 = BigQueryLoaderPolarsConfig(
            gcp_credential_key="test_key",
            project="test_project",
            dataset="test_dataset",
            table="test_table"
        )
        config3 = BigQueryLoaderPolarsConfig(
            gcp_credential_key="different",
            project="different",
            dataset="different",
            table="different"
        )
        
        assert config1 == config2
        assert config1 != config3