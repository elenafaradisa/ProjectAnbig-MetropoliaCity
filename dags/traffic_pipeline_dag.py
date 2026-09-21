from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='traffic_etl_pipeline',
    default_args=default_args,
    description='ETL Traffic Islamabad 2022',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['traffic', 'etl'],
) as dag:

    run_spark_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command=(
            "spark-submit "
            "--jars /opt/airflow/scripts/postgresql-42.6.0.jar "
            "--driver-class-path /opt/airflow/scripts/postgresql-42.6.0.jar "
            "/opt/airflow/scripts/spark_traffic_etl.py"
        ),
        execution_timeout=timedelta(minutes=45),
    )