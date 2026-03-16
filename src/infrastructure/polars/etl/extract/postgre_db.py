from src.templates.etl.extract.base import BaseExtractor
import polars as pl
import pyarrow as pa


class PostgreDBExtractorWithPolars(BaseExtractor):
    """
        Extract data from postgresql database using polars

        Parameters:
            query (str): SQL query to execute
            uri (str): Connection URI for the database
            **kwargs: Additional keyword arguments for polars.read_database_uri

        Returns:
            ArrowTable: Extracted data as an Arrow table
    """
    def __init__(self, query: str, uri: str, **kwargs) -> None:
        self.query = query
        self.uri = uri
        self.kwargs = kwargs

    def extract(self) -> pa.Table:
        df = pl.read_database_uri(query=self.query, uri=self.uri, engine="adbc", **self.kwargs)
        arrow_table = df.to_arrow()
        return arrow_table
