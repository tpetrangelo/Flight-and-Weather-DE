from datetime import datetime, timezone

from ingestion.utils.s3_keys import build_raw_key
from ingestion.utils.s3_writer import upload_json

BUCKET = "flight-weather-de-project"


def main():
    ts = datetime.now(timezone.utc)

    payload = {
        "source": "flightradar24",
        "message": "hello s3",
        "ts_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    key = build_raw_key("flightradar24", ts)
    upload_json(BUCKET, key, payload, metadata={"source": "flightradar24"})

    print(f"Uploaded: s3://{BUCKET}/{key}")


if __name__ == "__main__":
    main()
