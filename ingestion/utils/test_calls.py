from __future__ import annotations

from datetime import datetime, timezone, timedelta

from fr24sdk.client import Client

from app.config import FR24_API_KEY, AWS_S3_BUCKET, AWS_S3_FR24_SOURCE

BUCKET = AWS_S3_BUCKET
SOURCE = AWS_S3_FR24_SOURCE


def main():
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
        print("FR24 payload type:", type(payload))
        if isinstance(payload, dict):
            print("FR24 payload keys:", list(payload.keys())[:30])
            for k in ["data", "flights", "result", "items", "rows"]:
                v = payload.get(k)
                print(f"key={k} type={type(v)} len={len(v) if hasattr(v,'__len__') else 'na'}")
        else:
            print("FR24 payload repr:", repr(payload)[:500])

if __name__ == '__main__':
    main()
