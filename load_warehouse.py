"""
load_warehouse.py  (PHASE 3)
=========================
Reads traffic_cleaned.csv and loads it into the star-schema warehouse.

Load order (dimensions first so the facts can reference them):
    1. dim_location  - the 5 fixed Egyptian locations
    2. dim_weather   - unique weather conditions found in the data
    3. dim_time      - one row per source timestamp
    4. fact_traffic  - all 2000 rows
    5. fact_anomalies- only rows where anomaly_type != 'NORMAL'

The warehouse is emptied and its IDENTITY counters reseeded at the start,
so record_id / time_id line up 1:1 with the CSV row order. That lets us
link fact_anomalies.record_id to the matching fact_traffic row without an
extra round-trip per row.

Run:  python load_warehouse.py   (run warehouse_setup.py first)
"""

import sys
import time

import pandas as pd
import pyodbc

from config import check_required_env, get_sql_connection_string

CLEAN_FILE = "traffic_cleaned.csv"

# dim_location reference rows: (location_id, name, city, region)
LOCATIONS = [
    (1, "Ring Road Cairo", "Cairo", "Greater Cairo"),
    (2, "Corniche Alexandria", "Alexandria", "Mediterranean Coast"),
    (3, "Suez Road", "Cairo", "East Cairo"),
    (4, "October Bridge", "Cairo", "Greater Cairo"),
    (5, "Tahrir Square", "Cairo", "Central Cairo"),
]

# Human-readable descriptions per weather condition (fallback provided).
WEATHER_DESCRIPTIONS = {
    "Sunny": "Clear skies and good visibility",
    "Rainy": "Wet roads, reduced traction",
    "Foggy": "Low visibility conditions",
    "Cloudy": "Overcast but generally dry",
    "Snowy": "Snow on the road, hazardous driving",
    "Windy": "Strong winds, possible debris",
}


def reseed(cursor, conn):
    """Empty every table and reset IDENTITY counters so ids start at 1."""
    for table in ["fact_anomalies", "fact_traffic", "dim_time",
                  "dim_weather", "dim_location"]:
        cursor.execute(f"DELETE FROM {table}")
    conn.commit()
    # dim_location has no IDENTITY column, so it is not reseeded.
    for table in ["fact_anomalies", "fact_traffic", "dim_time", "dim_weather"]:
        cursor.execute(f"DBCC CHECKIDENT ('{table}', RESEED, 0)")
    conn.commit()


def main() -> None:
    check_required_env()

    print(f"Reading {CLEAN_FILE}...")
    try:
        df = pd.read_csv(CLEAN_FILE, parse_dates=["timestamp"])
    except FileNotFoundError:
        print(f"ERROR: '{CLEAN_FILE}' not found. Run data_cleaning.py first.")
        sys.exit(1)
    df = df.reset_index(drop=True)
    print(f"   {len(df)} rows loaded from CSV.\n")

    print("Connecting to Azure SQL Database...")
    try:
        conn = pyodbc.connect(get_sql_connection_string())
    except pyodbc.Error as exc:
        print(f"FAILED to connect: {exc}")
        sys.exit(1)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    print("Connected.\n")

    start = time.time()

    # Clean slate.
    print("Clearing existing warehouse data and reseeding ids...")
    reseed(cursor, conn)

    # ---- 1) dim_location ----
    print("Loading dim_location...")
    cursor.executemany(
        "INSERT INTO dim_location (location_id, location_name, city, region) "
        "VALUES (?, ?, ?, ?)",
        LOCATIONS,
    )
    conn.commit()

    # ---- 2) dim_weather ----
    print("Loading dim_weather...")
    weather_values = sorted(df["weather_condition"].dropna().unique().tolist())
    weather_rows = [
        (w, WEATHER_DESCRIPTIONS.get(w, "No description available"))
        for w in weather_values
    ]
    cursor.executemany(
        "INSERT INTO dim_weather (weather_condition, description) VALUES (?, ?)",
        weather_rows,
    )
    conn.commit()
    # Build weather_condition -> weather_id map from what was inserted.
    cursor.execute("SELECT weather_id, weather_condition FROM dim_weather")
    weather_map = {cond: wid for wid, cond in cursor.fetchall()}

    # ---- 3) dim_time (one row per CSV row, in order -> time_id = i + 1) ----
    print("Loading dim_time...")
    time_rows = []
    for ts in df["timestamp"]:
        ts = pd.Timestamp(ts)
        hour = int(ts.hour)
        is_peak = 1 if (7 <= hour <= 9 or 16 <= hour <= 19) else 0
        time_rows.append(
            (ts.to_pydatetime(), hour, ts.day_name(),
             int(ts.month), int(ts.year), is_peak)
        )
    cursor.executemany(
        "INSERT INTO dim_time (full_timestamp, hour, day_of_week, month, year, "
        "is_peak_hour) VALUES (?, ?, ?, ?, ?, ?)",
        time_rows,
    )
    conn.commit()

    # ---- 4) fact_traffic (in order -> record_id = i + 1) ----
    print("Loading fact_traffic...")
    fact_rows = []
    for i, row in df.iterrows():
        fact_rows.append((
            i + 1,                                   # time_id
            int(row["location_id"]),                 # location_id
            weather_map.get(row["weather_condition"]),  # weather_id
            int(row["traffic_volume"]),
            float(row["avg_vehicle_speed"]),
            int(row["vehicle_count_cars"]),
            int(row["vehicle_count_trucks"]),
            int(row["vehicle_count_bikes"]),
            int(row["total_vehicles"]),
            float(row["temperature"]),
            float(row["humidity"]),
            str(row["signal_status"]),
            str(row["speed_category"]),
        ))
    cursor.executemany(
        "INSERT INTO fact_traffic (time_id, location_id, weather_id, "
        "traffic_volume, avg_vehicle_speed, vehicle_count_cars, "
        "vehicle_count_trucks, vehicle_count_bikes, total_vehicles, "
        "temperature, humidity, signal_status, speed_category) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        fact_rows,
    )
    conn.commit()

    # ---- 5) fact_anomalies (non-NORMAL rows; record_id = i + 1) ----
    print("Loading fact_anomalies...")
    anomaly_rows = []
    for i, row in df.iterrows():
        if row["anomaly_type"] == "NORMAL":
            continue
        anomaly_rows.append((
            i + 1,                                   # record_id
            int(row["location_id"]),
            str(row["anomaly_type"]),
            str(row["severity"]),
            float(row["avg_vehicle_speed"]),
            int(row["traffic_volume"]),
            int(row["accident_reported"]),
            pd.Timestamp(row["timestamp"]).to_pydatetime(),  # detected_at
        ))
    if anomaly_rows:
        cursor.executemany(
            "INSERT INTO fact_anomalies (record_id, location_id, anomaly_type, "
            "severity, avg_vehicle_speed, traffic_volume, accident_reported, "
            "detected_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            anomaly_rows,
        )
        conn.commit()

    elapsed = time.time() - start

    # ---- Final summary ----
    def count(table):
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        return cursor.fetchone()[0]

    print("\n" + "=" * 55)
    print(" LOAD SUMMARY")
    print("=" * 55)
    print(f"  dim_location   : {count('dim_location')} rows")
    print(f"  dim_weather    : {count('dim_weather')} rows")
    print(f"  dim_time       : {count('dim_time')} rows")
    print(f"  fact_traffic   : {count('fact_traffic')} rows")
    print(f"  fact_anomalies : {count('fact_anomalies')} rows (anomalies)")
    print(f"  Time taken     : {elapsed:.1f} seconds")
    print("=" * 55)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
