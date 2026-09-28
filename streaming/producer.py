import argparse
import json
import time

import pandas as pd
from kafka import KafkaProducer

DEFAULT_CSV = "data/raw/traffictab23.csv"
DEFAULT_TOPIC = "traffic_topic"
DEFAULT_BOOTSTRAP = "localhost:9092"


def load_sorted_rows(csv_path: str, start: str | None, end: str | None, limit: int | None) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    df = df.sort_values("timestamp")

    if start:
        df = df[df["timestamp"] >= pd.Timestamp(start)]
    if end:
        df = df[df["timestamp"] < pd.Timestamp(end)]
    if limit:
        df = df.head(limit)

    return df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=DEFAULT_CSV)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--interval", type=float, default=1.0, help="jeda detik antar pesan")
    parser.add_argument("--limit", type=int, default=None, help="batasi jumlah baris (untuk demo)")
    parser.add_argument("--start", default=None, help="ISO date, mulai dari timestamp ini")
    parser.add_argument("--end", default=None, help="ISO date, berhenti sebelum timestamp ini")
    args = parser.parse_args()

    print(f"Membaca & mengurutkan {args.csv} berdasarkan timestamp...")
    df = load_sorted_rows(args.csv, args.start, args.end, args.limit)
    print(f"Akan mengirim {len(df)} baris, jeda {args.interval}s per baris.")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    for i, row in enumerate(df.to_dict(orient="records"), start=1):
        producer.send(args.topic, row)
        if i % 50 == 0 or i == len(df):
            print(f"[{i}/{len(df)}] terkirim, timestamp terakhir={row['timestamp']}")
        time.sleep(args.interval)

    producer.flush()
    print("Selesai mengirim semua baris.")


if __name__ == "__main__":
    main()