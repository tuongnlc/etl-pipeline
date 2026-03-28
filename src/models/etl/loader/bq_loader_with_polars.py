from dataclasses import dataclass

@dataclass
class BigQueryLoaderPolarsConfig:
    gcp_credential_key: str
    project: str
    dataset: str
    table: str