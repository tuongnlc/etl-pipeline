from dataclasses import dataclass

@dataclass
class BigQueryLoaderPolarsConfig:
    gcp_credential_key: str
    write_disposition: str 
    project: str
    dataset: str
    table: str
