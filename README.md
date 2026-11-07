# dsex

Download Dhaka Stock Exchange (DSE) day-end prices to Parquet. Data is available from July 2024.

```bash
uv sync
uv run python tidydse.py                          # add new days to data/dse_day_end.parquet
uv run python tidydse.py --start 2024-07-01       # backfill from a date
```
