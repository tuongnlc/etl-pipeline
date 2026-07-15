from underthesea import word_tokenize
import polars as pl


class TokenizeVi:
    def __init__(self, col_name: str | None, tokenize_col_name: str | None):
        self.col_name = col_name
        self.tokenize_col_name = tokenize_col_name

    def _tokenize_vi(self, text: str | None) -> str | None:
        if text is None:
            return None
        return word_tokenize(text, format="text")

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
            Select column and do tokenize_vi
        """

        col_name = self.col_name
        df = df.with_columns(
            pl.col(col_name)
            .map_elements(self._tokenize_vi, return_dtype=pl.String)
            .alias(self.tokenize_col_name)
        )
        return df
