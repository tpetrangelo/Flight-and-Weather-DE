from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Iterable

import time
import requests
import pandas as pd

from app.config import AERODATABOX_API_KEY, AWS_S3_BUCKET, AWS_S3_ADB_SOURCE
from ingestion.utils.s3_keys import build_raw_key
from ingestion.utils.s3_io import upload_parquet_to_s3  

BUCKET = AWS_S3_BUCKET
SOURCE = AWS_S3_ADB_SOURCE

def call_with_backoff(url, headers, querystring, max_retries=5, timeout=30) -> dict | list:
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers, params = querystring, timeout=timeout)

        if response.status_code == 200:
            return response.json()

        if response.status_code == 429:
            wait = 2 ** attempt  # exponential backoff
            time.sleep(wait)
            continue

        response.raise_for_status()

    raise RuntimeError("Max retries exceeded")


def _fetch_adb_records(airports: Iterable[str]) -> pd.DataFrame:
    """
    Fetch AeroDataBox departure data for multiple airports and normalize.
    Inject departure_airport_iata per record inside the loop.
    """
    all_departures: list[dict] = []
    now = datetime.now(timezone.utc)

    if isinstance(airports, str):
        airports = (airports,)

    for airport in airports:
        print(f"Calling API for {airport}")
        url = f"https://aerodatabox.p.rapidapi.com/flights/airports/Iata/{airport}"
        querystring = {"durationMinutes": "180", "direction": "Departure"}
        headers = {
            "Accept": "application/json",
            "X-RapidAPI-Key": AERODATABOX_API_KEY,
            "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
        }

        response = call_with_backoff(url, headers=headers, querystring=querystring, timeout=30)

        departures = response.get("departures", [])
        if not isinstance(departures, list):
            raise TypeError(f"Expected payload['departures'] to be a list, got {type(departures)}")

        # ✅ inject airport per record
        for d in departures:
            if isinstance(d, dict):
                d["departure_airport_iata"] = airport
                d["window_start_utc"] = now
                d["window_end_utc"] = now + timedelta(hours=6)
                d["source"] = "AeroDataBox"
                d["schema_version"] = "1.0"
                d["sched_dep_date"] = now.date().isoformat()
            else:
                raise TypeError(f"Expected each departure to be dict, got {type(d)}")

        # ✅ flatten (no nested list)
        all_departures.extend(departures)

    if not all_departures:
        raise ValueError("No departures returned from AeroDataBox for the provided airports")

    # Normalize after all records collected
    df = pd.json_normalize(all_departures, sep="__")
    return df



def run() -> str:
    """
    Pull ADB data, write a parquet file to S3, return the S3 key written.
    Designed to be called by Airflow.
    """

    if not AERODATABOX_API_KEY:
        raise RuntimeError("AERODATABOX_API_KEY not set")

    departure_airport_iata = ('BOS') #,'DEN','ORD','LAX','JFK','CLT','LAS','MCO','MIA')

    df = _fetch_adb_records(departure_airport_iata)

    if df is None or df.empty or df.columns.size == 0:
        raise ValueError("ADB returned no usable records (empty dataframe).")

    ts = datetime.now(timezone.utc)
    adb_key = build_raw_key(SOURCE, ts, ext="parquet")

    upload_parquet_to_s3(
        bucket=BUCKET,
        key=adb_key,
        df=df,
        metadata={"source": SOURCE},
    )

    print(f"Wrote ADB parquet to s3://{BUCKET}/{adb_key}")
    return adb_key


if __name__ == "__main__":
    # Local test run
    key = run()
    print("ADB S3 key:", key)
