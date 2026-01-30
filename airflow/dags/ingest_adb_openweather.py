from __future__ import annotations

from datetime import datetime, timedelta
import sys

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator # type: ignore

# ✅ MUST come BEFORE importing ingestion.*
PROJECT_PATH = "/opt/airflow/project"
if PROJECT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_PATH)

from ingestion.clients.aerodatabox_client import run as adb_run
from ingestion.clients.airportsinuse_client import run as airports_run
from ingestion.clients.openweather_client import run as openweather_run



default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_adb_and_openweather",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # schedule="0 */6 * * *",  # every 6 hours
    schedule = None,
    catchup=False,
    tags=["ingestion"],
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

    adb_to_s3 >> airports_to_s3 >> openweather_to_s3
