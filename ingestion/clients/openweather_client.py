from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any

import pandas as pd
import requests

from app.config import AWS_S3_BUCKET, AWS_S3_OPENWEATHER_SOURCE, OPENWEATHER_API_KEY
from ingestion.utils.s3_io import read_parquet_from_s3, upload_parquet_to_s3
from ingestion.utils.s3_keys import build_raw_key


BUCKET = AWS_S3_BUCKET
SOURCE = AWS_S3_OPENWEATHER_SOURCE


def fetch_weather(*, lat: float, lon: float) -> Dict[str, Any]:
    """
    Fetch current weather for a coordinate pair from OpenWeather.
    Returns raw JSON as dict.
    """
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": float(lat),
        "lon": float(lon),
        "appid": OPENWEATHER_API_KEY,
        "units": "metric",
    }

    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def fetch_weather_for_airports(airports_df: pd.DataFrame) -> pd.DataFrame:
    """
    Given airports dataframe with columns: airport_code, lat, lon
    Returns a flattened weather dataframe, 1 row per airport.
    """
    required = ["airport_code", "lat", "lon"]
    missing = [c for c in required if c not in airports_df.columns]
    if missing:
        raise ValueError(f"Missing columns in airports_df: {missing}. "
                         f"Available columns: {list(airports_df.columns)[:30]}")

    # Drop null/invalid coords
    df = airports_df.copy()
    df = df[df["lat"].notna() & df["lon"].notna()]
    df = df[df["airport_code"].astype(str).str.strip() != ""]

    obs_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = []
    for _, row in df.iterrows():
        weather = fetch_weather(lat=row["lat"], lon=row["lon"])

        # Be defensive with nested fields
        main = weather.get("main", {}) or {}
        wind = weather.get("wind", {}) or {}
        weather_list = weather.get("weather") or [{}]
        w0 = weather_list[0] if isinstance(weather_list, list) and weather_list else {}

        rows.append(
            {
                "airport_code": row["airport_code"],
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "obs_ts_utc": obs_ts,

                "temp_c": main.get("temp"),
                "feels_like_c": main.get("feels_like"),
                "pressure": main.get("pressure"),
                "humidity": main.get("humidity"),
                "wind_speed": wind.get("speed"),
                "wind_deg": wind.get("deg"),
                "visibility": weather.get("visibility"),
                "weather_main": w0.get("main"),
                "weather_desc": w0.get("description"),

                # Optional: nice for debugging / later joins
                "openweather_city_id": weather.get("id"),
            }
        )

    return pd.DataFrame(rows)


def run(airports_s3_key: str) -> str:
    """
    Read airports parquet from S3, fetch OpenWeather for each airport, write parquet to S3.
    Returns the S3 key written.
    """
    if not OPENWEATHER_API_KEY:
        raise RuntimeError("OPENWEATHER_API_KEY not set")

    airports_df = read_parquet_from_s3(BUCKET, airports_s3_key)
    if airports_df.empty:
        raise ValueError("No airports provided to OpenWeather client (airports_df is empty).")

    weather_df = fetch_weather_for_airports(airports_df)

    ts = datetime.now(timezone.utc)
    openweather_key = build_raw_key(SOURCE, ts, ext="parquet")

    upload_parquet_to_s3(
        bucket=BUCKET,
        key=openweather_key,
        df=weather_df,
        metadata={"source": SOURCE, "source_airports_key": airports_s3_key},
    )

    print(f"Wrote OpenWeather parquet to s3://{BUCKET}/{openweather_key}")
    return openweather_key
