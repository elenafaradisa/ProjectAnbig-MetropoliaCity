"""Data-quality checks that belong to the ETL stage itself (not the full
six-test analytical audit, which runs later in notebooks/01_audit_data.ipynb
and only matters for deciding dashboard scope).

These checks answer: did the load work correctly? Not: is the data
realistic? Results are appended to meta.quality_results so they can be
compared run over run.

Usage:
    python -m spark.jobs.quality_checks --run-id 2026-09-21T02:00 \
        --table core.observations
"""
import argparse
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from spark.common.schema import CORE_COLUMNS, RANGE_CHECKS
from spark.common.spark_session import get_spark, read_from_postgres, write_to_postgres


def run(run_id: str, table: str = "core.observations", spark=None) -> list:
    owns_session = spark is None
    if owns_session:
        spark = get_spark("quality_checks_traffictab23")
    df = read_from_postgres(spark, table)

    results = []

    def record(category, metric_name, value, detail=""):
        results.append(
            {
                "run_id": run_id,
                "category": category,
                "metric_name": metric_name,
                "value": float(value) if value is not None else None,
                "detail": detail,
            }
        )

    total = df.count()
    record("integrity", "row_count", total)

    dup_ts = df.groupBy("observed_at", "road_segment_id").count().filter("count > 1").count()
    record("integrity", "duplicate_observed_at_road", dup_ts,
           "rows sharing (observed_at, road_segment_id) — should be 0 if each 30s tick is one road")

    n_roads = df.select("road_segment_id").distinct().count()
    record("integrity", "distinct_road_segment_id", n_roads)

    for col in CORE_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            record("nulls", f"null_count.{col}", null_count, f"{null_count}/{total} rows")

    for col, (lo, hi) in RANGE_CHECKS.items():
        out_of_range = df.filter((F.col(col) < lo) | (F.col(col) > hi)).count()
        record("range", f"out_of_range.{col}", out_of_range, f"expected [{lo}, {hi}]")

    inconsistent = df.filter(~F.col("label_consistent")).count()
    record("consistency", "anomaly_incident_type_mismatch", inconsistent,
           "rows where (anomaly_label==1) != (incident_type!=0)")

    tod_mismatch_rate = df.filter(F.col("tod_mismatch")).count() / total if total else 0
    record("consistency", "tod_mismatch_rate", tod_mismatch_rate,
           "share of rows where raw time_of_day disagrees with the hour derived from the timestamp")

    dow_mismatch_rate = df.filter(F.col("dow_mismatch")).count() / total if total else 0
    record("consistency", "dow_mismatch_rate", dow_mismatch_rate,
           "share of rows where raw day_of_week disagrees with the weekday derived from the timestamp")

    anomaly_rate = df.filter(F.col("anomaly_label") == 1).count() / total if total else 0
    record("audit", "anomaly_rate", anomaly_rate)

    print(f"Quality check run {run_id}: {len(results)} metrics recorded.")
    for r in results:
        print(f"  [{r['category']}] {r['metric_name']} = {r['value']}  {r['detail']}")

    result_df = spark.createDataFrame(results)
    write_to_postgres(result_df, "meta.quality_results", mode="append")

    if owns_session:
        spark.stop()
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--table", default="core.observations")
    args = parser.parse_args()
    run_id = args.run_id or f"{datetime.now(timezone.utc).isoformat()}-{uuid.uuid4().hex[:8]}"
    run(run_id, args.table)


if __name__ == "__main__":
    main()
