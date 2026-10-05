"""ETL DAG for the Islamabad pollution feature: ingest (xlsx/csv ->
core.pollution_hourly).

Small, historical air-quality + weather dataset (~32K hourly rows total
between the two source files), so this pipeline is intentionally plain
Python (pandas + psycopg2) rather than PySpark -- see pollution/ingest.py's
module docstring for why.

Overlap handling (the training and testing batches share ~3,623 hours) is
done inside ingest.py itself: training rows always upsert, testing rows
insert with ON CONFLICT DO NOTHING, so core.pollution_hourly ends up with a
clean, leakage-free data_split='testing' held-out set for model evaluation.

Model training (ml/train_pollution_model.py) is NOT a task here -- it's run
manually, since the data is historical and doesn't change on its own, so
retraining on every DAG run would just burn CPU for no benefit (see that
script's docstring). This DAG's `forecast` task only loads the model it
already saved and scores it against current core.pollution_hourly.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/project"
TRAINING_FILE = f"{PROJECT_ROOT}/data/raw/pollution/islamabad_training.xlsx"
TESTING_FILE = f"{PROJECT_ROOT}/data/raw/pollution/islamabad_testing.csv"
MODEL_DIR = f"{PROJECT_ROOT}/data/models"

default_args = {
    "owner": "metropolia",
    "retries": 1,
}

with DAG(
    dag_id="etl_pollution",
    description="Ingest Islamabad hourly air-quality + weather data into core.pollution_hourly",
    default_args=default_args,
    schedule=None,  # historical batch load, triggered manually for this course deliverable
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "pollution", "islamabad"],
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command=(
            f"cd {PROJECT_ROOT} && python -m pollution.ingest "
            f"--training {TRAINING_FILE} --testing {TESTING_FILE}"
        ),
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=(
            f"cd {PROJECT_ROOT} && python -m pollution.quality_checks "
            f"--run-id etl_pollution__{{{{ run_id }}}}"
        ),
    )

    forecast = BashOperator(
        task_id="forecast",
        bash_command=(
            f"cd {PROJECT_ROOT} && python -m pollution.forecast "
            f"--model-dir {MODEL_DIR}"
        ),
    )

    ingest >> quality_checks >> forecast