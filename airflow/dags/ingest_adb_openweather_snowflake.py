from __future__ import annotations

from datetime import datetime, timedelta
import sys
import os

import snowflake.connector

from airflow import DAG

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator # type: ignore


PROJECT_PATH = "/opt/airflow/project"
if PROJECT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_PATH)

from ingestion.clients.aerodatabox_client import run as adb_run
from ingestion.clients.airportsinuse_client import run as airports_run
from ingestion.clients.openweather_client import run as openweather_run

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")

def _load_private_key_der() -> bytes:
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if not key_path:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH is not set")

    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    password_bytes = passphrase.encode() if passphrase else None

    with open(key_path, "rb") as f:
        pkey = serialization.load_pem_private_key(
            f.read(),
            password=password_bytes,
            backend=default_backend(),
        )

    return pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _snowflake_connect():
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER", "AIRFLOW_ETL_USER")
    if not account:
        raise ValueError("SNOWFLAKE_ACCOUNT is not set")

    return snowflake.connector.connect(
        account=account,
        user=user,
        private_key=_load_private_key_der(),
        authenticator="snowflake",
        warehouse="FLIGHT_WEATHER_INGEST_WH",
        role="ROLE_FLIGHT_WEATHER_ETL",
        database="FLIGHT_WEATHER",
        schema="BRONZE",
    )


def _run_snowflake_sql_file(sql_path: str) -> None:
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    conn = _snowflake_connect()
    try:
        # Best way to run multi-statement SQL in Snowflake connector:
        for cur in conn.execute_string(sql_text):
            # Consume results if any to avoid "results pending" edge cases
            try:
                cur.fetchall()
            except Exception:
                pass
    finally:
        conn.close()


def _test_snowflake_connection() -> None:
    conn = _snowflake_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();"
            )
            print(cur.fetchone())
    finally:
        conn.close()



default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SNOWFLAKE_INGEST_DIR = os.path.join(PROJECT_PATH, "snowflake", "ingest")

FLIGHTS_SQL_PATH = os.path.join(SNOWFLAKE_INGEST_DIR, "07_copy_into_flights.sql")
WEATHER_SQL_PATH = os.path.join(SNOWFLAKE_INGEST_DIR, "08_copy_into_weather.sql")
VERIFY_SQL_PATH  = os.path.join(SNOWFLAKE_INGEST_DIR, "09_verify_loads.sql")


for p in [SNOWFLAKE_INGEST_DIR, FLIGHTS_SQL_PATH, WEATHER_SQL_PATH, VERIFY_SQL_PATH]:
    if not os.path.exists(p):
        raise FileNotFoundError(f"Missing inside Airflow container: {p}")


with DAG(
    dag_id="ingest_adb_and_openweather_snowflake",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # schedule="0 */6 * * *",  # every 6 hours
    schedule = None,
    catchup=False,
    tags=["ingestion","snowflake"],
) as dag:

    def _adb_to_s3(**context) -> str:
        # should return the s3 key it wrote
        return adb_run()

    def _airports_to_s3(ti, **context) -> str:
        adb_key = ti.xcom_pull(task_ids="adb_to_s3")
        return airports_run(adb_s3_key=adb_key)

    def _openweather_to_s3(ti, **context) -> str:
        airports_key = ti.xcom_pull(task_ids="airports_to_s3")
        return openweather_run(airports_s3_key=airports_key)

    adb_to_s3 = PythonOperator(
        task_id="adb_to_s3",
        python_callable=_adb_to_s3,
    )

    airports_to_s3 = PythonOperator(
        task_id="airports_to_s3",
        python_callable=_airports_to_s3,
    )

    openweather_to_s3 = PythonOperator(
        task_id="openweather_to_s3",
        python_callable=_openweather_to_s3,
    )


    # ----------------
    # Snowflake loading
    # ----------------


    test_snowflake = PythonOperator(
        task_id="test_snowflake_connection",
        python_callable=_test_snowflake_connection,
    )

    copy_flights_bronze = PythonOperator(
        task_id="copy_into_flights_bronze",
        python_callable=_run_snowflake_sql_file,
        op_kwargs={"sql_path": FLIGHTS_SQL_PATH},
    )

    copy_weather_bronze = PythonOperator(
        task_id="copy_into_weather_bronze",
        python_callable=_run_snowflake_sql_file,
        op_kwargs={"sql_path": WEATHER_SQL_PATH},
    )

    verify_loads = PythonOperator(
        task_id="verify_snowflake_loads",
        python_callable=_run_snowflake_sql_file,
        op_kwargs={"sql_path": VERIFY_SQL_PATH},
    )



    # ------------
    # Dependencies
    # ------------
    adb_to_s3 >> airports_to_s3 >> openweather_to_s3
    openweather_to_s3 >> test_snowflake
    test_snowflake >> [copy_flights_bronze, copy_weather_bronze]
    [copy_flights_bronze, copy_weather_bronze] >> verify_loads
