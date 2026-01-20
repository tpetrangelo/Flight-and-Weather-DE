# Ingestion
## Ingestion - Utils
### From ingestion.utils.s3_keys.py

```python

# Tells python to stop trying to understand type hints (like x: int) immediately, and instead just treat them as "plain text" (strings) until sometihng else (like a code editor or type-checker) needs them.
from __future__ import annotations

def build_raw_key(
    source: str,

    # Union pipe, ts either takes a datetime or if nothing is passed, then defaults to None
    ts: datetime | None = None,
    
    # Indicates that anything following the * needs to be indicated by keyword. When calling the function a user would need to provide build_raw_key("data", my_time, include_minute = False) vs build_raw_key("data", my_time, False)
    *,

    # True will be default value if nothing is passed in
    include_minute: bool = True,
    ext: str = "parquet",

# Indicates that return type is string
) -> str:
   
```
### From ingestion.utils.s3_io.py
``` python

# Decorator for storing data, Python will automatically create the __init__ constructor, __repr__ string display, __eq__ equality comparison. frozen=True makes the object immutable
@dataclass(frozen=True)
class S3Location:
    bucket: str
    key: str


# _ function name means it's mean to be private/internal
def _s3_client():
    return boto3.client("s3")

# bucket: str, key: str are hints, telling Python that args should be strings. -> is the return type hint saying the function should return type pd.Dataframe
def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
   

def upload_parquet_to_s3(
    *,
    bucket: str,
    key: str,
    df: pd.DataFrame,
    compression: str = "snappy",
    content_type: str = "application/x-parquet",

    # This parameter should be a dictionary with string keys and values or None
    #Could be metadata: dict[str,str] | None = None
    metadata: Optional[dict[str, str]] = None,
) -> None

    # Convert DF -> Arrow Table -> parquet bytes
    table = pa.Table.from_pandas(df, preserve_index=False)
    
    # Buffer that lives in memory not hard drive
    buffer = BytesIO()
    
    # Moves cursor to beginning of buffer
    buffer.seek(0)

    # Metadata (optional, but nice for lineage/debugging)
    meta = {"ingest_ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    
    # If metadata is not empty/None/generally truthy
    if metadata:
        # Loops through each kVp, converts key/value to strings and adds them to meta dictionary
        meta.update({str(k): str(v) for k, v in metadata.items()})

def upload_bytes(
    *,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str = "application/octet-stream",

    # Could be metadata: dict[str,str] | None = None
    metadata: Optional[dict[str, str]] = None,
) -> None:
```

## Ingestion - Clients
### From ingestion.clients.flightradar24_client.py
``` python

# Returns a list where each element is a dict
# Function starting with _ to identify function is interal/private. Not enforced but signals intent
def _fetch_fr24_records() -> list[dict]:

        # Converts a Pydantic model into a regular Python dict. model_dump() = Pydantic v2
        payload = response.model_dump()

    # Normalize to list-of-records (adjust if your SDK shape differs)
    records = payload.get("data") or payload.get("flights") or payload

    #Checks if first var is an instance of second var
    if isinstance(records, dict):
        # sometimes APIs return {id: {...}, id2: {...}}
        records = list(records.values())

    if not isinstance(records, list):
        raise ValueError(f"Unexpected FR24 records type: {type(records)}")

    return records


def run() -> str:

    # Take a list of dicts and turns to a df   
    df = pd.DataFrame.from_records(records)
```

### From ingestion.clients.airportsinuse_client.py
```python

# Takes in pandas df return pandas df
def extract_airports_in_use(flights_df: pd.DataFrame) -> pd.DataFrame:
    
    # Adjust column names to match your FR24 schema if needed
    required_cols = ["origin", "destination"]
    
    # Builds a list of all required columns that are NOT present in the DataFrame
    missing = [c for c in required_cols if c not in flights_df.columns]
    if missing:
        raise ValueError(f"Missing columns in flights_df: {missing}. "
                         f"Available columns: {list(flights_df.columns)[:30]}")

    # Gets unique airports and flattens to a 1D list
    airport_codes = pd.unique(flights_df[required_cols].values.ravel())
    
    # Drop null/empty/non-strings
    airport_codes = [c for c in airport_codes if isinstance(c, str) and c.strip()]

```

### From ingestion.clients.openweather_client.py
``` python

# Takes in floats and returns a Dict of key string: value Any (API Response)
def fetch_weather(*, lat: float, lon: float) -> Dict[str, Any]:
    
    # Checks the response code and raises an exception if 400-401 etc.
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def fetch_weather_for_airports(airports_df: pd.DataFrame) -> pd.DataFrame:
    
    required = ["airport_code", "lat", "lon"]
    
    # Checks if airport_code, lat, lon are missing from dataframe passed in
    missing = [c for c in required if c not in airports_df.columns]
    if missing:
        raise ValueError(f"Missing columns in airports_df: {missing}. "
                         f"Available columns: {list(airports_df.columns)[:30]}")

    # Drop null/invalid coords
    # Keep only rows where lat and lon are not null
    df = df[df["lat"].notna() & df["lon"].notna()]

    # Remove rows where airport code is empty
    df = df[df["airport_code"].astype(str).str.strip() != ""]

    # Throwaway variable for _, do not need data on index
    for _, row in df.iterrows():
        weather = fetch_weather(lat=row["lat"], lon=row["lon"])
```

# Docker
## Docker - Airflow
### From docker.airflow.Dockerfile

``` Docker

# Start from an official Apache Airflow image (version 2.9.3), using Python 3.11
FROM apache/airflow:2.9.3-python3.11

# Switch to user airflow (non-root)
USER airflow

# Container needs requirements for depencies 
COPY requirements.txt /requirements.txt

# Install the required python packages. --no-cache-dir keeps the image smaller by not storing pips download cache.
RUN pip install --no-cache-dir -r /requirements.txt

```
### From docker.docker-compose.yaml
``` Yaml

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - airflow_pgdata:/var/lib/postgresql/data

  airflow-init:
    build:
      context: ..
      dockerfile: docker/airflow/Dockerfile
    depends_on:
      - postgres
    env_file:
      - ../.env
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__WEBSERVER__SECRET_KEY: "REDACTED"
    volumes:
      - ../airflow/dags:/opt/airflow/dags
      - ../:/opt/airflow/project
      - ~/.aws:/home/airflow/.aws:ro
      - airflow_logs:/opt/airflow/logs
    entrypoint: /bin/bash
    command:
      - -lc
      - |
        set -e
        airflow db init

        airflow users create \
          --username admin \
          --password admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com || true

        airflow users reset-password --username admin --password admin

  airflow-webserver:
    build:
      context: ..
      dockerfile: docker/airflow/Dockerfile
    depends_on:
      - postgres
    env_file:
      - ../.env
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true"
      AIRFLOW__WEBSERVER__SECRET_KEY: "REDACTED"

      # Logs (shared volume)
      AIRFLOW__LOGGING__BASE_LOG_FOLDER: /opt/airflow/logs
      AIRFLOW__LOGGING__REMOTE_LOGGING: "false"

      # AWS (either via ~/.aws mount OR via env vars from .env)
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION:-us-east-2}
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-}
      AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN:-}

    volumes:
      - ../airflow/dags:/opt/airflow/dags
      - ../:/opt/airflow/project
      - ~/.aws:/home/airflow/.aws:ro
      - airflow_logs:/opt/airflow/logs
    ports:
      - "8080:8080"
    command: webserver

  airflow-scheduler:
    build:
      context: ..
      dockerfile: docker/airflow/Dockerfile
    depends_on:
      - postgres
    env_file:
      - ../.env
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__WEBSERVER__SECRET_KEY: "REDACTED"

      AIRFLOW__LOGGING__BASE_LOG_FOLDER: /opt/airflow/logs
      AIRFLOW__LOGGING__REMOTE_LOGGING: "false"

      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION:-us-east-2}
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-}
      AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN:-}

    volumes:
      - ../airflow/dags:/opt/airflow/dags
      - ../:/opt/airflow/project
      - ~/.aws:/home/airflow/.aws:ro
      - airflow_logs:/opt/airflow/logs
    command: scheduler

volumes:
  airflow_pgdata:
  airflow_logs:


```
