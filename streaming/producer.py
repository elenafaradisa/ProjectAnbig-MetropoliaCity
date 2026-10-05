"""Replay traffictab23 into Kafka, in timestamp order.

Two pacing modes:
  - steady (default): one event, then sleep --interval seconds;
  - burst: every second, send a random number of events between
    --burst-min and --burst-max (until --limit is reached), to show the
    pipeline coping with an uneven load. Enabled by giving --burst-max.
"""
import argparse
import json
import random
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
    parser.add_argument("--burst-min", type=int, default=1,
                        help="mode burst: jumlah event minimum per detik")
    parser.add_argument("--burst-max", type=int, default=None,
                        help="mode burst: jumlah event maksimum per detik (mengaktifkan mode burst)")
    parser.add_argument("--seed", type=int, default=None, help="seed acak untuk mode burst (opsional)")
    args = parser.parse_args()

    print(f"Membaca & mengurutkan {args.csv} berdasarkan timestamp...")
    df = load_sorted_rows(args.csv, args.start, args.end, args.limit)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    rows = df.to_dict(orient="records")

    if args.burst_max:
        if args.burst_min < 1 or args.burst_max < args.burst_min:
            raise SystemExit("--burst-min harus >= 1 dan <= --burst-max")
        rnd = random.Random(args.seed)
        print(f"Mode burst: {len(rows)} baris, {args.burst_min}-{args.burst_max} event per detik.")
        sent, second = 0, 0
        while sent < len(rows):
            started = time.monotonic()
            k = min(rnd.randint(args.burst_min, args.burst_max), len(rows) - sent)
            for row in rows[sent:sent + k]:
                producer.send(args.topic, row)
            producer.flush()  # the whole burst leaves within this second
            sent += k
            second += 1
            print(f"[detik {second}] {k} event terkirim, total {sent}/{len(rows)}, "
                  f"timestamp terakhir={rows[sent - 1]['timestamp']}")
            time.sleep(max(0.0, 1.0 - (time.monotonic() - started)))
    else:
        print(f"Akan mengirim {len(rows)} baris, jeda {args.interval}s per baris.")
        for i, row in enumerate(rows, start=1):
            producer.send(args.topic, row)
            if i % 50 == 0 or i == len(rows):
                print(f"[{i}/{len(rows)}] terkirim, timestamp terakhir={row['timestamp']}")
            time.sleep(args.interval)

    producer.flush()
    print("Selesai mengirim semua baris.")


if __name__ == "__main__":
    main()
