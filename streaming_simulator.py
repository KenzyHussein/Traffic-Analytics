"""
streaming_simulator.py  (PHASE 4)
=================================
Streams the REAL cleaned dataset (traffic_cleaned.csv) one row per second.

For each row it:
  - sends the row as a JSON event to Azure Event Hubs (all CSV columns plus
    a fresh event_id UUID and streamed_at local timestamp),
  - writes the row to fact_traffic in Azure SQL (dual write),
  - if the row is an anomaly (anomaly_type != 'NORMAL'), also writes it to
    fact_anomalies, linked by the fact_traffic record_id,
  - prints a one-line status every second,
  - prints a rolling summary every 60 seconds.

When it reaches the last row it loops back to row 0. Handles Ctrl+C
gracefully and reconnects automatically on Event Hub / SQL failures.

Run:  python streaming_simulator.py
(run data_cleaning.py, warehouse_setup.py and load_warehouse.py first)
"""

import json
import time
import uuid
from collections import Counter
from datetime import datetime

import numpy as np
import pandas as pd
import pyodbc
from azure.eventhub import EventData, EventHubProducerClient

from config import (
    EVENT_HUB_CONNECTION_STR,
    EVENT_HUB_NAME,
    check_required_env,
    get_sql_connection_string,
)

CLEAN_FILE = "traffic_cleaned.csv"
SUMMARY_INTERVAL = 60  # seconds


def to_native(value):
    """Convert numpy scalars to plain Python types so json can serialise them."""
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    return value


class StreamingSimulator:
    def __init__(self):
        self.producer = None
        self.sql_conn = None
        self.weather_map = {}

        # Rolling stats for the 60-second summary.
        self.window_events = 0
        self.window_anomalies = 0
        self.window_locations = Counter()
        self.last_summary_time = time.time()

    # ------------------------------------------------------------------ #
    # Connections (with reconnection)
    # ------------------------------------------------------------------ #
    def connect_event_hub(self):
        try:
            if self.producer is not None:
                try:
                    self.producer.close()
                except Exception:
                    pass
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
            )
            print("[CONN] Event Hub producer connected.")
        except Exception as exc:
            print(f"[CONN][ERROR] Event Hub connect failed: {exc}")
            self.producer = None

    def connect_sql(self):
        try:
            if self.sql_conn is not None:
                try:
                    self.sql_conn.close()
                except Exception:
                    pass
            self.sql_conn = pyodbc.connect(get_sql_connection_string())
            print("[CONN] Azure SQL connected.")
            self.load_weather_map()
        except Exception as exc:
            print(f"[CONN][ERROR] SQL connect failed: {exc}")
            self.sql_conn = None

    def load_weather_map(self):
        """Cache weather_condition -> weather_id from dim_weather."""
        try:
            cur = self.sql_conn.cursor()
            cur.execute("SELECT weather_id, weather_condition FROM dim_weather")
            self.weather_map = {cond: wid for wid, cond in cur.fetchall()}
        except Exception:
            self.weather_map = {}

    def ensure_connections(self):
        if self.producer is None:
            self.connect_event_hub()
        if self.sql_conn is None:
            self.connect_sql()

    # ------------------------------------------------------------------ #
    # Send / write
    # ------------------------------------------------------------------ #
    def build_event(self, row):
        """Turn a DataFrame row into a JSON-safe event dict."""
        event = {col: to_native(row[col]) for col in row.index}
        event["event_id"] = str(uuid.uuid4())
        event["streamed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return event

    def send_to_event_hub(self, event):
        try:
            if self.producer is None:
                self.connect_event_hub()
            if self.producer is None:
                return False
            batch = self.producer.create_batch()
            batch.add(EventData(json.dumps(event)))
            self.producer.send_batch(batch)
            return True
        except Exception as exc:
            print(f"[HUB][ERROR] send failed, will reconnect: {exc}")
            self.producer = None
            return False

    def write_to_sql(self, row, event):
        """Insert into fact_traffic (and fact_anomalies if needed).

        Returns True on success. Uses OUTPUT INSERTED.record_id to link the
        anomaly row back to the fact_traffic row it came from.
        """
        try:
            if self.sql_conn is None:
                self.connect_sql()
            if self.sql_conn is None:
                return False

            cursor = self.sql_conn.cursor()
            weather_id = self.weather_map.get(row["weather_condition"])

            # fact_traffic (time_id is left NULL for live events)
            cursor.execute(
                "INSERT INTO fact_traffic (time_id, location_id, weather_id, "
                "traffic_volume, avg_vehicle_speed, vehicle_count_cars, "
                "vehicle_count_trucks, vehicle_count_bikes, total_vehicles, "
                "temperature, humidity, signal_status, speed_category) "
                "OUTPUT INSERTED.record_id "
                "VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                int(row["location_id"]), weather_id,
                int(row["traffic_volume"]), float(row["avg_vehicle_speed"]),
                int(row["vehicle_count_cars"]), int(row["vehicle_count_trucks"]),
                int(row["vehicle_count_bikes"]), int(row["total_vehicles"]),
                float(row["temperature"]), float(row["humidity"]),
                str(row["signal_status"]), str(row["speed_category"]),
            )
            record_id = cursor.fetchone()[0]

            # fact_anomalies (only for non-NORMAL rows)
            if row["anomaly_type"] != "NORMAL":
                cursor.execute(
                    "INSERT INTO fact_anomalies (record_id, location_id, "
                    "anomaly_type, severity, avg_vehicle_speed, traffic_volume, "
                    "accident_reported, detected_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    record_id, int(row["location_id"]),
                    str(row["anomaly_type"]), str(row["severity"]),
                    float(row["avg_vehicle_speed"]), int(row["traffic_volume"]),
                    int(row["accident_reported"]), datetime.now(),
                )

            self.sql_conn.commit()
            return True
        except Exception as exc:
            print(f"[SQL][ERROR] write failed, will reconnect: {exc}")
            self.sql_conn = None
            return False

    # ------------------------------------------------------------------ #
    # Summary
    # ------------------------------------------------------------------ #
    def print_summary(self):
        most_active = (
            self.window_locations.most_common(1)[0][0]
            if self.window_locations else "—"
        )
        print("\n--- 60s Summary ---")
        print(f"Events sent: {self.window_events}")
        print(f"Anomalies detected: {self.window_anomalies}")
        print(f"Most active location: {most_active}")
        print("---\n")
        self.window_events = 0
        self.window_anomalies = 0
        self.window_locations.clear()
        self.last_summary_time = time.time()

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #
    def run(self):
        check_required_env()

        print("Reading cleaned dataset...")
        try:
            df = pd.read_csv(CLEAN_FILE)  # timestamp kept as string for JSON
        except FileNotFoundError:
            print(f"ERROR: '{CLEAN_FILE}' not found. Run data_cleaning.py first.")
            return
        total = len(df)

        print("=" * 60)
        print(" Real-Time Traffic Analytics - Streaming Simulator")
        print(f" Streaming {total} real rows, 1/sec. Press Ctrl+C to stop.")
        print("=" * 60 + "\n")

        self.ensure_connections()

        idx = 0
        try:
            while True:
                row = df.iloc[idx]
                event = self.build_event(row)

                hub_ok = self.send_to_event_hub(event)
                sql_ok = self.write_to_sql(row, event)

                is_anomaly = row["anomaly_type"] != "NORMAL"

                # Rolling stats
                self.window_events += 1
                self.window_locations[row["location_name"]] += 1
                if is_anomaly:
                    self.window_anomalies += 1

                # Console line
                hub_flag = "HUB:OK" if hub_ok else "HUB:--"
                sql_flag = "SQL:OK" if sql_ok else "SQL:--"
                line = (
                    f"[ROW {idx + 1:03d}/{total}] {hub_flag} {sql_flag} | "
                    f"{row['location_name']} | speed={float(row['avg_vehicle_speed']):.1f} | "
                    f"vol={int(row['traffic_volume'])} | {row['anomaly_type']}"
                )
                if is_anomaly:
                    line += f" ({row['severity']}) **"
                print(line)

                # 60-second summary
                if time.time() - self.last_summary_time >= SUMMARY_INTERVAL:
                    self.print_summary()

                # Advance / loop back to the start
                idx += 1
                if idx >= total:
                    idx = 0
                    print("[LOOP] Restarting from row 0...")

                time.sleep(1)

        except KeyboardInterrupt:
            print("\n\nStopping streaming simulator (Ctrl+C received)...")
        finally:
            self.cleanup()

    def cleanup(self):
        if self.producer is not None:
            try:
                self.producer.close()
                print("[CONN] Event Hub producer closed.")
            except Exception:
                pass
        if self.sql_conn is not None:
            try:
                self.sql_conn.close()
                print("[CONN] SQL connection closed.")
            except Exception:
                pass
        print("Streaming simulator stopped. Goodbye.")


if __name__ == "__main__":
    StreamingSimulator().run()
