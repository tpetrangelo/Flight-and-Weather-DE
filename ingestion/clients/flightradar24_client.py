from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pandas as pd
from fr24sdk.client import Client

from app.config import FR24_API_KEY, AWS_S3_BUCKET, AWS_S3_FR24_SOURCE
from ingestion.utils.s3_keys import build_raw_key
from ingestion.utils.s3_io import upload_parquet_to_s3  # <-- new IO utility


BUCKET = AWS_S3_BUCKET
SOURCE = AWS_S3_FR24_SOURCE


def _fetch_fr24_records() -> list[dict]:
    """
    Fetch FR24 flight summary data and normalize to a list of records.
    """
    with Client(api_token=FR24_API_KEY) as client:
        # FR24 SDK appears to want naive timestamps (no tzinfo). Keep consistent.
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        future_naive = now_naive + timedelta(hours=6)

        response = client.flight_summary.get_full(
            routes=["US-US"],
            flight_datetime_from=now_naive.isoformat(timespec="seconds"),
            flight_datetime_to=future_naive.isoformat(timespec="seconds"),
        )

        payload = response.model_dump()

    # Normalize to list-of-records (adjust if your SDK shape differs)
    records = payload.get("data") or payload.get("flights") or payload
    if isinstance(records, dict):
        # sometimes APIs return {id: {...}, id2: {...}}
        records = list(records.values())

    if not isinstance(records, list):
        raise ValueError(f"Unexpected FR24 records type: {type(records)}")
    


    return records


def run() -> str:
    """
    Pull FR24 data, write a parquet file to S3, return the S3 key written.
    Designed to be called by Airflow.
    """


    if not FR24_API_KEY:
        raise RuntimeError("FR24_API_KEY not set")


    records = _fetch_fr24_records()
    df = pd.DataFrame.from_records(records)

    if df is None or df.empty or df.columns.size == 0:
        raise ValueError("FR24 returned no usable records (empty dataframe).")

    ts = datetime.now(timezone.utc)
    fr24_key = build_raw_key(SOURCE, ts, ext="parquet")

    upload_parquet_to_s3(
        bucket=BUCKET,
        key=fr24_key,
        df=df,
        metadata={"source": SOURCE},
    )

    print(f"Wrote FR24 parquet to s3://{BUCKET}/{fr24_key}")
    return fr24_key


if __name__ == "__main__":
    # Local test run
    key = run()
    print("FR24 S3 key:", key)
