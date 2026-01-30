from __future__ import annotations

from datetime import datetime, timezone, timedelta

import requests
import pandas as pd

from app.config import AERODATABOX_API_KEY, AWS_S3_BUCKET, AWS_S3_ADB_SOURCE
from ingestion.utils.s3_keys import build_raw_key
from ingestion.utils.s3_io import upload_parquet_to_s3  # <-- new IO utility


BUCKET = AWS_S3_BUCKET
SOURCE = AWS_S3_ADB_SOURCE


def _fetch_adb_records() -> pd.DataFrame:
    """
    Fetch ADB flight summary data and normalize to a list of records.
    """
    departure_airport_iata = 'BOS'
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/Iata/{departure_airport_iata}"
    querystring = {"durationMinutes":"360","direction":"Departure"}
    headers = {
        "Accept": "application/json, application/xml",
        "X-RapidAPI-Key": f"{AERODATABOX_API_KEY}",
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    #print(type(response))
    payload = response.json()
    departures = payload.get("departures", [])
    if not isinstance(departures, list):
        raise TypeError(f"Expected payload['departures'] to be a list, got {type(departures)}")

    df = pd.json_normalize(departures, sep="__")  # sep avoids dots in column names


    now = datetime.now(timezone.utc)
    df["departure_airport_iata"] = departure_airport_iata
    df["window_start"] = now
    df["window_end_utc"] = now + timedelta(hours=6)
    df["source"] = "AeroDataBox"
    df["schema_version"] = "1.0"
    df["sched_dep_date"] = now.date().isoformat()

    # Normalize to list-of-records (adjust if your SDK shape differs)
    # records = payload.get("departures") or payload.get("flights") or payload
    # if isinstance(records, dict):
    #     # sometimes APIs return {id: {...}, id2: {...}}
    #     records = list(records.values())

    # if not isinstance(records, list):
    #     raise ValueError(f"Unexpected ADB records type: {type(records)}")
    
    return df


def run() -> str:
    """
    Pull ADB data, write a parquet file to S3, return the S3 key written.
    Designed to be called by Airflow.
    """


    if not AERODATABOX_API_KEY:
        raise RuntimeError("AERODATABOX_API_KEY not set")


    df = _fetch_adb_records()
    print(df)
    #df = pd.DataFrame.from_records(records)

    if df is None or df.empty or df.columns.size == 0:
        raise ValueError("ADB returned no usable records (empty dataframe).")

    ts = datetime.now(timezone.utc)
    adb_key = build_raw_key(SOURCE, ts, ext="parquet", departure_airport_iata = df["departure_airport_iata"].iloc[0])

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
