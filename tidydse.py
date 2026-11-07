"""Fetch, clean, and maintain DSE day-end archive data.

Examples:
    python tidydse.py
    python tidydse.py --start 2024-07-01 --end 2025-06-30
"""

import argparse
import re
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.dse.com.bd/api/live/data-archive/day-end"
MAX_PAGES = 500
CHUNK_DAYS = 182  # long ranges are fetched in ~half-year chunks
DROP_COLUMNS = ["serial"]
DATE_COLUMN = "date"
KEY_COLUMNS = ["date", "trading_code"]
DEFAULT_PATH = Path("data") / "dse_day_end.parquet"

HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://www.dse.com.bd/data-archives",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36"
    ),
}


def _to_date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _snake(name: str) -> str:
    """Convert camelCase names to snake_case."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()


def _clean(df: pl.DataFrame) -> pl.DataFrame:
    """Drop unwanted columns, snake_case names, and parse the date column."""
    df = df.drop(DROP_COLUMNS, strict=False)
    df = df.rename({c: _snake(c) for c in df.columns if _snake(c) != c})
    if df.schema.get(DATE_COLUMN) == pl.String:
        df = df.with_columns(pl.col(DATE_COLUMN).str.to_date("%Y-%m-%d"))
    return df


def _extract_rows(payload) -> list[dict]:
    """Find the list of row dictionaries in a JSON payload of unknown shape."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "items", "results", "rows", "docs", "records"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict) and (nested := _extract_rows(value)):
                return nested
        for value in payload.values():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return value
    return []


def _session() -> requests.Session:
    """HTTP session with browser headers and retries on transient errors."""
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(total=5, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _fetch_rows(session: requests.Session, start: date, end: date) -> list[dict]:
    """Page through the API for one date range."""
    rows: list[dict] = []
    previous: list[dict] | None = None
    for page in range(1, MAX_PAGES + 1):
        params = {"from": start.isoformat(), "to": end.isoformat(), "page": page}
        response = session.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        page_rows = _extract_rows(response.json()) if response.content.strip() else []
        if not page_rows or page_rows == previous:
            break
        rows.extend(page_rows)
        previous = page_rows
    return rows


def _chunks(start: date, end: date, days: int = CHUNK_DAYS):
    """Yield (chunk_start, chunk_end) pairs covering start..end inclusive."""
    while start <= end:
        chunk_end = min(start + timedelta(days=days - 1), end)
        yield start, chunk_end
        start = chunk_end + timedelta(days=1)


def get_day_end_archive(
    start: date | str,
    end: date | str,
    schema: pl.Schema | dict | None = None,
) -> pl.DataFrame:
    """Return day-end rows between start and end, inclusive, sorted by date and code."""
    start, end = _to_date(start), _to_date(end)
    if start > end:
        raise ValueError(f"start {start} is after end {end}")

    rows: list[dict] = []
    with _session() as session:
        for chunk_start, chunk_end in _chunks(start, end):
            rows.extend(_fetch_rows(session, chunk_start, chunk_end))

    if not rows:
        return pl.DataFrame(schema=schema)
    df = _clean(pl.DataFrame(rows, infer_schema_length=None))
    return df.unique(subset=KEY_COLUMNS, keep="last").sort(KEY_COLUMNS)


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    """Write atomically so an interrupted run never corrupts the dataset."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.write_parquet(tmp)
    tmp.replace(path)


def update_dataset(
    path: str | Path = DEFAULT_PATH,
    start: date | str | None = None,
    end: date | str | None = None,
) -> pl.DataFrame:
    """Fetch new rows into a Parquet dataset, de-duplicating by date and trading code.

    By default fetches from the day after the latest stored date through today.
    If the file does not exist, fetches today's data.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _clean(pl.read_parquet(path)) if path.exists() else None

    end = _to_date(end) if end else date.today()
    if start:
        start = _to_date(start)
    elif existing is not None and existing.height:
        start = existing[DATE_COLUMN].max() + timedelta(days=1)
    else:
        start = date.today()

    empty = existing if existing is not None else pl.DataFrame()
    if start > end:
        print(f"Already up to date (start {start} is after end {end}).")
        return empty

    new = get_day_end_archive(start, end, schema=existing.schema if existing is not None else None)
    if new.is_empty():
        print(f"No data for {start} to {end} (holiday or not published yet).")
        return empty

    combined = new if existing is None else pl.concat([existing, new], how="diagonal_relaxed")
    combined = combined.unique(subset=KEY_COLUMNS, keep="last").sort(KEY_COLUMNS)
    _write_parquet(combined, path)
    added = combined.height - (existing.height if existing is not None else 0)
    print(f"Fetched {start} to {end}: added {added} rows; total {combined.height}.")
    return combined


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--start", help="First date to fetch (YYYY-MM-DD)")
    parser.add_argument("--end", help="Last date to fetch (YYYY-MM-DD)")
    parser.add_argument("--path", default=str(DEFAULT_PATH), help="Dataset Parquet path")
    args = parser.parse_args()
    update_dataset(path=args.path, start=args.start, end=args.end)


if __name__ == "__main__":
    main()
