from etl_pipeline.templates.etl.load.base import BaseLoader

class DatabricksVolumeLoader(BaseLoader):
    def load(self) -> None:
        raise NotImplementedError("load method must be implemented")