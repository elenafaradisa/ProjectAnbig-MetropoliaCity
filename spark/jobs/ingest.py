"""Extract: read the raw traffictab23 CSV and write it out as Parquet,
partitioned by year/month, with corrected types.

This is the "E" in ETL. No feature derivation happens here — that is
transform.py's job — so a bug in transform logic can be fixed and rerun
without re-reading the (large) source CSV.

Malformed-row handling: this does NOT use CSV's columnNameOfCorruptRecord or
mode="DROPMALFORMED". Both were tried and found unreliable for CSV in this
Spark build (3.5.1): a row with too few fields, or a value that fails to
cast to its declared type, does not populate _corrupt_record; and
DROPMALFORMED produced a mismatch between df.count() (2) and df.collect()
(1 row) on the same DataFrame in testing — a real correctness bug, not just
a cosmetic one. Instead: read with default PERMISSIVE mode (bad values
become null, consistent for both engines) and drop any row where at least
one required column is null, which is what a short/mistyped row causes.

Usage:
    python -m spark.jobs.ingest --input /opt/project/data/raw/traffictab23.csv \
        --output /opt/project/data/parquet
"""
import argparse
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from spark.common.schema import RAW_COLUMNS, RAW_SCHEMA
from spark.common.spark_session import get_spark


def run(input_path: str, output_path: str, spark=None) -> int:
    """spark: pass an existing SparkSession (e.g. from a pytest fixture) to
    reuse it; when None, a new session is created and left running (the
    caller — main() — is responsible for stopping it)."""
    owns_session = spark is None
    if owns_session:
        spark = get_spark("ingest_traffictab23")

    # StructType(list(...)) clones the field list rather than reusing the
    # shared RAW_SCHEMA object directly — harmless here since we don't
    # mutate it, but keeps this call independent of any future schema.add()
    # elsewhere in the same process.
    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(StructType(list(RAW_SCHEMA.fields)))
        .csv(input_path)
    )

    total_read = df.count()

    not_null_condition = None
    for col in RAW_COLUMNS:
        cond = F.col(col).isNotNull()
        not_null_condition = cond if not_null_condition is None else (not_null_condition & cond)
    df = df.filter(not_null_condition)

    row_count = df.count()
    dropped = total_read - row_count
    if dropped > 0:
        print(f"WARNING: {dropped} row(s) had a missing/unparseable field and were dropped.")

    df = df.withColumn("timestamp", F.to_timestamp("timestamp")).withColumn(
        "obs_year", F.year("timestamp")
    ).withColumn("obs_month", F.month("timestamp"))

    # to_timestamp can itself return null for a string that doesn't match
    # the expected format even though every raw field was present; catch
    # that separately from the missing-field case above.
    unparseable_ts = df.filter(F.col("timestamp").isNull()).count()
    if unparseable_ts > 0:
        print(f"WARNING: {unparseable_ts} row(s) had an unparseable timestamp and were dropped.")
        df = df.filter(F.col("timestamp").isNotNull())
        row_count -= unparseable_ts

    print(f"Ingested {row_count} rows (read {total_read}).")

    (
        df.write.mode("overwrite")
        .partitionBy("obs_year", "obs_month")
        .parquet(output_path)
    )
    print(f"Wrote Parquet to {output_path}")

    if owns_session:
        spark.stop()
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    row_count = run(args.input, args.output)
    if row_count == 0:
        sys.exit("Ingest produced 0 rows — check --input path.")


if __name__ == "__main__":
    main()
