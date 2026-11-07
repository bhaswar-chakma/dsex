import polars as pl
df = pl.read_parquet("data/dse_day_end.parquet")
print(df.select("date").min())
print(df.select("date").max())