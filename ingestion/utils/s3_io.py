from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True)
class S3Location:
    bucket: str
    key: str


def _s3_client():
    # Centralize this so you can later add config (region, retries, etc.)
    return boto3.client("s3")


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Reads a parquet object from S3 into a pandas DataFrame.
    """
    s3 = _s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    return pd.read_parquet(BytesIO(data))


def upload_parquet_to_s3(
    *,
    bucket: str,
    key: str,
    df: pd.DataFrame,
    compression: str = "snappy",
    content_type: str = "application/x-parquet",
    metadata: Optional[dict[str, str]] = None,
) -> None:
    """
    Converts a pandas DataFrame to parquet (in-memory) and uploads to S3.

    - No temp files
    - Consistent parquet writing via pyarrow
    - Optional S3 object metadata
    """
    # Convert DF -> Arrow Table -> parquet bytes
    table = pa.Table.from_pandas(df, preserve_index=False)

    buffer = BytesIO()
    pq.write_table(table, buffer, compression=compression)
    buffer.seek(0)

    # Metadata (optional, but nice for lineage/debugging)
    meta = {"ingest_ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    if metadata:
        meta.update({str(k): str(v) for k, v in metadata.items()})

    s3 = _s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.read(),
        ContentType=content_type,
        Metadata=meta,
    )


def upload_bytes(
    *,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str = "application/octet-stream",
    metadata: Optional[dict[str, str]] = None,
) -> None:
    """
    Generic uploader for non-parquet payloads (optional utility).
    """
    meta = {"ingest_ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    if metadata:
        meta.update({str(k): str(v) for k, v in metadata.items()})

    s3 = _s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
        Metadata=meta,
    )
