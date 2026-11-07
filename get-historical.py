from tidydse import get_day_end_archive
import polars as pl

output_file = "data/dse_day_end.parquet"

# Fetch data by half yearly period------------------------

## 2026 H1
df_26_h1 = get_day_end_archive("2026-01-01", "2026-06-30")

## 2025
df_25_h1 = get_day_end_archive("2025-01-01", "2025-06-30")
df_25_h2 = get_day_end_archive("2025-07-01", "2025-12-31")

## 2024
df_24_h2 = get_day_end_archive("2024-07-01", "2024-12-31")


# Concatenate---------------------------------------------
df = pl.concat([
    # new dse site contains data from 20241107
    df_24_h2,
    df_25_h1, df_25_h2,
    df_26_h1
])

# Write----------------------------------------------------
df.write_parquet(output_file)



