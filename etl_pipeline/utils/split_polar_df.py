import polars as pl


def split_dataframe(df: pl.DataFrame, n: int) -> list[pl.DataFrame]:
    """
        Split DataFrame into multiple small DataFrames.
    """
    q, r = divmod(len(df), n)
    return [df.slice(i * q + min(i, r), q + (1 if i < r else 0)) for i in range(n)]