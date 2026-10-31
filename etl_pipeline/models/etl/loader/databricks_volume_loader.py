from dataclasses import dataclass

@dataclass
class DatabricksVolumeLoaderConfig:
    volume_url: str
    local_file_path: str
