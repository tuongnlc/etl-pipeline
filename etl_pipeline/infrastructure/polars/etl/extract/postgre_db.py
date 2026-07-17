import polars as pl

from etl_pipeline.templates.etl.extract.postgre_db import PostgreDBExtractor
# import logger
from typing import Optional


class PostgreDBExtractorWithPolars(PostgreDBExtractor):
    """
        Extract data from postgresql database using polars

        Parameters:
            query (str): SQL query to execute
            uri (str): Connection URI for the database
            **kwargs: Additional keyword arguments for polars.read_database_uri

        Returns:
            polars.DataFrame: Extracted data as a polars DataFrame
    """
    def __init__(self, 
            source_table_name: str,
            uri: str, 
            filter_type: Optional[str]=None,
            execution_date_filter: str = None, #
            filter_time_range: str = 90, #Default behaviour is get 90 days data
            extractor_column_filter: Optional[str] = None,
            filter_value: Optional[str] = None,            
            **kwargs
        ) -> None:
        self.source_table_name = source_table_name
        self.uri = uri
        self.execution_date_filter = execution_date_filter
        self.filter_time_range = filter_time_range
        self.extractor_column_filter = extractor_column_filter
        self.filter_value = filter_value
        self.filter_type = filter_type
        self.kwargs = kwargs

    def extract(self) -> pl.DataFrame:
        #Build query query will be select * with filter by date if execution_date is not None        
        query = f"""
            SELECT 
                *
            FROM {self.source_table_name}
        """

        #Query by execution_date using for postgre_bq etl
        if self.filter_type == 'date':
            # if self.execution_date_filter:
            import logging
            logger = logging.getLogger(__name__)
            
            logger.info(f"Execution date: {self.execution_date_filter}")
            query += f" WHERE DATE(trading_date) >= DATE('{self.execution_date_filter}') - INTERVAL '{self.filter_time_range} days'"
        else:
        # Query for postgre_qrant etl
            if self.extractor_column_filter:
                query += f" WHERE {self.extractor_column_filter} = {self.filter_value}"
        print(query)

        df = pl.read_database_uri(query=query, uri=self.uri, engine="adbc", **self.kwargs)
        df = df.with_columns(pl.col("id").bin.encode("hex").alias("id"))
        return df
