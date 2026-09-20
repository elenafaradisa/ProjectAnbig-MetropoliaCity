"""ETL DAG for traffictab23: ingest (CSV -> Parquet) -> transform+load
(Parquet -> core.observations) -> quality_checks (-> meta.quality_results).

This is the "sampai ETL" scope for this week: extract, transform, load, and
the quality checks that belong to the load step itself. The full six-test
analytical audit and mart-building run separately (later stage), because
their scope depends on this DAG's output.

Each task shells out to `python -m spark.jobs.<name>` inside the Airflow
container, which has PySpark and the Postgres JDBC driver installed
(docker/airflow/Dockerfile). Using BashOperator + module invocation, rather
than SparkSubmitOperator, keeps this runnable without a separate Spark
master/worker service.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/project"
RAW_CSV = f"{PROJECT_ROOT}/data/raw/traffictab23.csv"
PARQUET_DIR = f"{PROJECT_ROOT}/data/parquet"
CONFIG_PATH = f"{PROJECT_ROOT}/config/incident_types.yaml"

default_args = {
    "owner": "metropolia",
    "retries": 1,
}

with DAG(
    dag_id="etl_traffic",
    description="Ingest, transform, load, and quality-check traffictab23",
    default_args=default_args,
    schedule=None,  # triggered manually for this course deliverable; not a live feed
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "traffictab23"],
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command=(
            f"cd {PROJECT_ROOT} && python -m spark.jobs.ingest "
            f"--input {RAW_CSV} --output {PARQUET_DIR}"
        ),
    )

    transform_load = BashOperator(
        task_id="transform_load",
        bash_command=(
            f"cd {PROJECT_ROOT} && python -m spark.jobs.transform "
            f"--input {PARQUET_DIR} --config {CONFIG_PATH}"
        ),
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=(
            f"cd {PROJECT_ROOT} && python -m spark.jobs.quality_checks "
            f"--run-id etl_traffic__{{{{ run_id }}}}"
        ),
    )

    ingest >> transform_load >> quality_checks
