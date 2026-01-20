from __future__ import annotations
from datetime import datetime, timezone

def build_raw_key(
    source: str,
    ts: datetime | None = None,
    *,
    include_minute: bool = True,
    ext: str = "parquet",
) -> str:
    if ts is None:
        ts = datetime.now(timezone.utc)
    elif ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)

    ext = ext.lstrip(".").lower()

    dt_part = f"dt={ts:%Y-%m-%d}"
    hr_part = f"hr={ts:%H}"
    parts = [f"bronze/{source}", dt_part, hr_part]

    if include_minute:
        parts.append(f"min={ts:%M}")

    filename = f"{source}_{ts:%Y%m%dT%H%M%SZ}.{ext}"
    return "/".join(parts + [filename])
