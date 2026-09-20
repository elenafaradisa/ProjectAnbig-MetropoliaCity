import pandas as pd, numpy as np

df['timestamp'] = pd.to_datetime(df['timestamp'])
df['hour'] = df['timestamp'].dt.hour
df['month'] = df['timestamp'].dt.month

# 0. Sanity check
print(df.shape, df.isna().sum().sum())
print("Rasio anomali:", df['anomaly_label'].mean())
print(pd.crosstab(df['anomaly_label'], df['incident_type']))   # cek leakage

# 1. Apakah ada struktur kemacetan? (kecepatan, okupansi, kepadatan, jumlah kendaraan)
cong = ['vehicle_count','average_speed','lane_occupancy_rate','jam_density_index',
        'stop_duration_avg','hard_braking_events','rapid_acceleration_events']
print(df[cong].corr().round(3))

# 2. Apakah GPS terikat ke road_segment_id? (std ~0.029 = acak uniform di kotak 0.1 derajat)
print(df.groupby('road_segment_id')[['gps_latitude','gps_longitude']].std().mean())

# 3. Apakah time_of_day konsisten dengan jam?
print(pd.crosstab(df['hour'], df['time_of_day'], normalize='index').round(2))

# 4. Apakah cuaca punya musim? (Snow di Juli = acak)
print(pd.crosstab(df['month'], df['weather_condition'], normalize='index').round(3))

# 5. Fitur mana yang berbeda antara anomali vs normal?
feat = ['vehicle_count','average_speed','lane_occupancy_rate','jam_density_index',
        'hard_braking_events','rapid_acceleration_events','lane_changes_per_minute',
        'stop_duration_avg','visibility_range','v2x_packet_loss_rate',
        'v2v_beacon_interval_avg','v2x_message_delay_avg']
print(df.groupby('anomaly_label')[feat].mean().T.round(3))

# 6. Pola harian anomali (ada efek jam sibuk atau tidak?)
print(df.groupby('hour')['anomaly_label'].mean().round(3))