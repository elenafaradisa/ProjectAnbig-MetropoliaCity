import csv
import os

from spark.jobs.ingest import run
from spark.common.schema import RAW_COLUMNS

GOOD_ROW = [
    "2022-01-01 00:00:00", "Afternoon", "Saturday", "33.5525", "72.9775",
    "1028", "30.06", "18", "55.32", "0.192", "75.36", "0", "1", "5.45",
    "70.86", "Clear", "2882.93", "Dry", "4.68", "0.73", "353.65", "0", "0",
]


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(RAW_COLUMNS)
        for r in rows:
            w.writerow(r)


def test_ingest_reads_valid_rows_and_partitions_by_year_month(spark, tmp_path):
    csv_path = tmp_path / "in.csv"
    out_path = tmp_path / "out.parquet"
    write_csv(csv_path, [GOOD_ROW])

    row_count = run(str(csv_path), str(out_path), spark=spark)

    assert row_count == 1
    # partitionBy(obs_year, obs_month) creates obs_year=2022/obs_month=1/ dirs
    assert os.path.isdir(out_path)
    partitions = os.listdir(out_path)
    assert any(p.startswith("obs_year=2022") for p in partitions)


def test_ingest_drops_rows_with_missing_fields(spark, tmp_path, capsys):
    csv_path = tmp_path / "in.csv"
    out_path = tmp_path / "out.parquet"
    # second row is missing trailing fields (e.g. a truncated write) -> the
    # missing columns parse as null and the row is dropped
    short_row = GOOD_ROW[:4]
    write_csv(csv_path, [GOOD_ROW, short_row])

    row_count = run(str(csv_path), str(out_path), spark=spark)

    assert row_count == 1
    captured = capsys.readouterr()
    assert "WARNING" in captured.out


def test_ingest_drops_rows_with_unparseable_timestamp(spark, tmp_path, capsys):
    csv_path = tmp_path / "in.csv"
    out_path = tmp_path / "out.parquet"
    bad_row = list(GOOD_ROW)
    bad_row[0] = "not-a-timestamp"
    write_csv(csv_path, [GOOD_ROW, bad_row])

    row_count = run(str(csv_path), str(out_path), spark=spark)

    assert row_count == 1
    captured = capsys.readouterr()
    assert "unparseable timestamp" in captured.out
