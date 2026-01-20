from __future__ import annotations

from datetime import datetime, timezone
import pandas as pd
import airportsdata

from ingestion.utils.s3_io import read_parquet_from_s3, upload_parquet_to_s3
from ingestion.utils.s3_keys import build_raw_key

from app.config import AWS_S3_BUCKET, AWS_S3_AIRPORTSINUSE_SOURCE


BUCKET = AWS_S3_BUCKET
WRITE_SOURCE = AWS_S3_AIRPORTSINUSE_SOURCE


def extract_airports_in_use(flights_df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a flights dataframe, return unique airports with lat/lon.
    Expects flights_df to have columns that identify origin/destination airport codes.
    """
    # Adjust column names to match your FR24 schema if needed
    required_cols = ["origin", "destination"]
    missing = [c for c in required_cols if c not in flights_df.columns]
    if missing:
        raise ValueError(f"Missing columns in flights_df: {missing}. "
                         f"Available columns: {list(flights_df.columns)[:30]}")

    airport_codes = pd.unique(flights_df[required_cols].values.ravel())
    # Drop null/empty/non-strings
    airport_codes = [c for c in airport_codes if isinstance(c, str) and c.strip()]

    airports = airportsdata.load("icao")

    rows = []
    for code in airport_codes:
        info = airports.get(code)
        if not info:
            continue
        rows.append(
            {
                "airport_code": code,
                "lat": info["lat"],
                "lon": info["lon"],
                # Optional extras (uncomment if you want them)
                # "name": info.get("name"),
                # "country": info.get("country"),
            }
        )

    return pd.DataFrame(rows)


def run(fr24_s3_key: str) -> str:
    """
    Read FR24 parquet from S3, extract airports in use + lat/lon, write airports parquet to S3.
    Returns the S3 key written.
    """
    flights_df = read_parquet_from_s3(BUCKET, fr24_s3_key)
    airports_df = extract_airports_in_use(flights_df)

    ts = datetime.now(timezone.utc)
    airport_key = build_raw_key(WRITE_SOURCE, ts, ext="parquet")

    upload_parquet_to_s3(
        bucket=BUCKET,
        key=airport_key,
        df=airports_df,
        metadata={"source": WRITE_SOURCE, "source_fr24_key": fr24_s3_key},
    )

    print(f"Wrote airports-in-use parquet to s3://{BUCKET}/{airport_key}")
    return airport_key
