"""Shared SparkSession builder and Postgres JDBC helpers.

Local (single-JVM) mode is the default, matching the architecture decision
to skip a separate Spark master/worker unless a cluster is explicitly
required by the course.
"""
import os

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "3g")
    jdbc_jar = os.environ.get("JDBC_JAR")  # set in docker/airflow/Dockerfile

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "Asia/Karachi")
    )
    if jdbc_jar:
        builder = builder.config("spark.jars", jdbc_jar)

    return builder.getOrCreate()


def postgres_jdbc_url() -> str:
    host = os.environ["POSTGRES_HOST"]
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    return f"jdbc:postgresql://{host}:{port}/{db}"


def postgres_properties() -> dict:
    return {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def write_to_postgres(df, table: str, mode: str = "append") -> None:
    """table must be schema-qualified, e.g. 'core.observations'.

    mode="overwrite" sets JDBC's truncate option so the existing table
    (with its indexes, created from sql/schemas/) is kept and just emptied,
    rather than Spark dropping and recreating it without those indexes.
    """
    writer = df.write
    if mode == "overwrite":
        writer = writer.option("truncate", "true")
    writer.jdbc(
        url=postgres_jdbc_url(),
        table=table,
        mode=mode,
        properties=postgres_properties(),
    )


def read_from_postgres(spark: SparkSession, table: str):
    return spark.read.jdbc(
        url=postgres_jdbc_url(),
        table=table,
        properties=postgres_properties(),
    )
