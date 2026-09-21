from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

TRAFFIC_PATH  = "/opt/airflow/data/TrafficTab23.csv"
POLLUTION_PATH = "/opt/airflow/data/Training/islamabad_complete_data.xlsx"

JDBC_URL   = "jdbc:postgresql://target-postgres:5432/traffic_warehouse"
JDBC_PROPS = {
    "user": "de_user",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}

YEAR_START = "2022-01-01"
YEAR_END   = "2022-12-31"

spark = SparkSession.builder \
    .appName("TrafficETL_2022") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session started")

print("📥 Extracting traffic data...")
df_traffic_raw = spark.read.csv(TRAFFIC_PATH, header=True, inferSchema=True)
print(f"   Raw rows: {df_traffic_raw.count()}")

print("📥 Extracting pollution data...")
pdf_poll = pd.read_excel(POLLUTION_PATH, engine="openpyxl")

pdf_poll = pdf_poll.rename(columns={
    'main.aqi'           : 'aqi',
    'components.pm2_5'   : 'pm2_5',
    'components.pm10'    : 'pm10',
    'components.co'      : 'co',
    'components.no'      : 'no_col',
    'components.no2'     : 'no2',
    'components.o3'      : 'o3',
    'components.so2'     : 'so2',
    'components.nh3'     : 'nh3',
    'temperature_2m'     : 'temperature',
    'wind_speed_10m'     : 'wind_speed',
    'wind_direction_10m' : 'wind_deg',
})
df_pollution = spark.createDataFrame(pdf_poll)
print(f"   Raw rows: {df_pollution.count()}")

print("🔄 Cleaning traffic data...")
df_traffic = df_traffic_raw \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .filter(F.col("timestamp").between(YEAR_START, YEAR_END)) \
    .filter(F.col("jam_density_index").isNotNull()) \
    .filter(F.col("average_speed") > 0)
print(f"   After clean: {df_traffic.count()} rows")

print("🔄 Resampling traffic to hourly...")
df_traffic = df_traffic \
    .withColumn("hour", F.date_trunc("hour", F.col("timestamp")))

df_traffic_h = df_traffic.groupBy("hour", "road_segment_id").agg(
    F.round(F.avg("average_speed"), 2).alias("avg_speed"),
    F.round(F.avg("jam_density_index"), 2).alias("avg_jam_density"),
    F.round(F.avg("lane_occupancy_rate"), 2).alias("avg_occupancy"),
    F.sum("incident_type").cast("int").alias("total_incidents"),
    F.sum("hard_braking_events").cast("int").alias("total_hard_braking")
)
print(f"   After resample: {df_traffic_h.count()} rows")

print("🔄 Cleaning pollution data...")
pdf_pollution = df_pollution \
    .withColumn("hour", F.date_trunc("hour", F.to_timestamp("datetime"))) \
    .filter(F.col("hour").between(YEAR_START, YEAR_END)) \
    .select("hour", "aqi", "pm2_5", "pm10", "temperature", "wind_speed") \
    .toPandas()

df_pollution = spark.createDataFrame(pdf_pollution)
print(f"   After clean: {df_pollution.count()} rows")

print("🔄 Joining traffic + pollution...")
df_merged = df_traffic_h.join(df_pollution, on="hour", how="left")
print(f"   After join: {df_merged.count()} rows")

# Forward-fill kolom polusi setelah join (per road_segment_id)
print("🔄 Forward-fill missing pollution values...")
poll_cols = ["aqi", "pm2_5", "pm10", "temperature", "wind_speed"]
pdf_merged = df_merged.orderBy("hour", "road_segment_id").toPandas()
pdf_merged[poll_cols] = pdf_merged.groupby("road_segment_id")[poll_cols].ffill().bfill()
df_merged = spark.createDataFrame(pdf_merged)
print(f"   After ffill: {df_merged.count()} rows")

print("🔄 Feature engineering...")
window_spec = Window.partitionBy("road_segment_id").orderBy("hour")

df_final = df_merged \
    .withColumn("hour_of_day", F.hour("hour")) \
    .withColumn("day_of_week", F.dayofweek("hour")) \
    .withColumn("is_weekend", F.dayofweek("hour").isin([1, 7])) \
    .withColumn("incident_flag", F.col("total_incidents") > 0) \
    .withColumn("target_jam_next_hour",
                F.lead("avg_jam_density", 1).over(window_spec)) \
    .dropna(subset=["target_jam_next_hour"])
print(f"   Final rows: {df_final.count()}")

print("💾 Loading to PostgreSQL...")
df_final.select(
    "hour", "road_segment_id",
    "avg_speed", "avg_jam_density", "avg_occupancy",
    "total_incidents", "total_hard_braking",
    "aqi", "pm2_5", "pm10", "temperature", "wind_speed",
    "hour_of_day", "day_of_week", "is_weekend",
    "incident_flag", "target_jam_next_hour"
).write.jdbc(
    url=JDBC_URL,
    table="traffic_features",
    mode="overwrite",
    properties=JDBC_PROPS
)

print("✅ ETL selesai! Data berhasil di-load ke PostgreSQL")
spark.stop()