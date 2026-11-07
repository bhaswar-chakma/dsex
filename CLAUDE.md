# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Scrapes Dhaka Stock Exchange (DSE) day-end price archives from `https://www.dse.com.bd/api/live/data-archive/day-end` and stores them as Parquet using Polars. Python 3.14, managed with `uv`.

## Commands

```bash
uv sync                                        # install deps
uv run python tidydse.py                       # append new days since last stored date
uv run python tidydse.py --start 2024-07-01    # backfill (DSE archive starts July 2024)
```

No test suite, linter, or formatter is configured. `.github/workflows/update-data.yml` runs the update on a schedule and commits the Parquet file.

## Architecture

All logic lives in `tidydse.py`.

- `get_day_end_archive(start, end)` — splits long ranges into ~half-year chunks (`CHUNK_DAYS`), paginates each (`page=1..MAX_PAGES`) until an empty or repeated page, then cleans, de-duplicates and sorts. HTTP retries are set up in `_session`. The response shape is undocumented, so `_extract_rows` searches the JSON for the row list.
- `_clean` — drops `serial`, converts camelCase columns to snake_case, parses `date` to `pl.Date`. Applied to both freshly fetched and existing data so schemas line up.
- `update_dataset` — incremental update of `data/dse_day_end.parquet`: fetches from the day after the latest stored date, concatenates with `how="diagonal_relaxed"`, and de-duplicates on `(date, trading_code)` keeping the latest.

Requests send browser-like headers (`HEADERS`); the API may reject requests without them. Parquet writes go through `_write_parquet` (write to a temp file, then rename) so an interrupted run can't corrupt the data.
