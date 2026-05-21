import polars as pl
from src.templates.etl.transform.base import TransformStep



class CleanTextPolars(TransformStep):
    def __init__(self, col_name: str = "newspaper_content") -> None:
        self.col_name = col_name

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
            Remove footer metadata from the article text.
            Normalize tabs into single spaces.
            Trim leading and trailing whitespace from the full text.
            Split text into lines for line-level cleaning.
            Preserve blank lines to keep paragraph structure.
            Replace special characters with spaces while keeping Unicode letters/digits.
            Collapse repeated spaces and tabs into a single space.
            Trim whitespace around each cleaned line.
            Join cleaned lines back into a single multiline string.
        """
        
        footer_pattern = r"[^\n]+\n+FILI\n+- \d{2}:\d{2} \d{2}/\d{2}/\d{4}"

        col_name = self.col_name

        return df.with_columns(
            pl.col(col_name)
            .fill_null("")
            .str.replace(footer_pattern, "", literal=False)
            .str.replace_all(r"\t+", " ", literal=False)
            .str.strip_chars()
            .str.split("\n")
            .list.eval(
                pl.when(pl.element().str.strip_chars() == "")
                .then(pl.lit(""))
                .otherwise(
                    pl.element()
                    .str.replace_all(r"[^\p{L}\p{N}_ \t]", " ", literal=False)
                    .str.replace_all(r"[ \t]+", " ", literal=False)
                    .str.strip_chars()
                )
            )
            .list.join("\n")
            .alias(col_name)
        )
