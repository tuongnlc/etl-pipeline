from etl_pipeline.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars


extractor_ = PostgreDBExtractorWithPolars(
    source_table_name = 'public.stock_price',
    uri='postgresql://postgres:postgres@localhost:5432/market_data',
    execution_date_filter='2026-07-01',
    filter_time_range='15' #extract data in sevend ay
)

df_ = extractor_.extract()
print(len(df_))


MAPPING_DICT = {
    "stock_id" : "giá cổ phiếu",
    "trading_date": "ngày giao dịch",
    "open_price": "giá mở cửa",
    "high_price": "giá cao nhất trong ngày",
    "low_price": "giá thấp nhất trong ngày",
    "close_price": "giá đóng cửa",
    "volume": "khối lượng giao dịch"
}

df_ = df_.rename(MAPPING_DICT)

exclude_cols = ["id"]
cols_to_concat = [col for col in df_.columns if col not in exclude_cols]

import polars as pl
#cast all column to string to concate



exprs = []
for col in cols_to_concat:
    # Nếu là cột ngày tháng (Date hoặc Datetime), ta định dạng cụ thể thành YYYY-MM-DD
    if df_[col].dtype in [pl.Date, pl.Datetime]:
        formatted_col = pl.col(col).dt.strftime("%Y-%m-%d")
        # print(df_[col])
        # formatted_col = pl.col(col).cast(pl.String)
    else:
        # Các kiểu dữ liệu khác ép kiểu String bình thường
        formatted_col = pl.col(col).cast(pl.String)
        
    exprs.append(pl.format("{}: {}", pl.lit(col), formatted_col))

df_new = df_.select([
    pl.col("id"),
    pl.concat_str(exprs, separator=", ").alias("content"),
    # pl.w
])
print(df_new.head())

df_ = df_new.to_dicts()
# # print(len(df_))



for i in df_:
    print(i)
#     print(type(i))
    break

