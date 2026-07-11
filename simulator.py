"""
simulator.py
------------
Real-Time Traffic Analytics - traffic data generator.

For 5 Egyptian locations it:
  - generates one realistic vehicle event every second,
  - sends each event as JSON to Azure Event Hubs,
  - writes each event to the traffic_events table in Azure SQL,
  - detects anomalies (speeding / congestion / accident risk) and writes
    them to the anomaly_log table,
  - every 60 seconds writes a per-location summary to location_summary.

It has try/except error handling with automatic reconnection for both the
Event Hub and the SQL Database, prints what it is doing in real time, and
runs until you press Ctrl+C.
"""

import json
import time
import uuid
from collections import defaultdict
from datetime import datetime

import pyodbc
from azure.eventhub import EventData, EventHubProducerClient
from faker import Faker

from config import (
    EVENT_HUB_CONNECTION_STR,
    EVENT_HUB_NAME,
    check_required_env,
    get_sql_connection_string,
)

fake = Faker()

# ---- Domain constants ----
LOCATIONS = [
    {"name": "Ring Road Cairo",     "lat": 30.0444, "lon": 31.2357},
    {"name": "Corniche Alexandria", "lat": 31.2001, "lon": 29.9187},
    {"name": "Suez Road",           "lat": 30.0131, "lon": 31.4731},
    {"name": "October Bridge",      "lat": 30.0561, "lon": 31.2394},
    {"name": "Tahrir Square",       "lat": 30.0444, "lon": 31.2357},
]

# Anomaly thresholds
SPEEDING_THRESHOLD = 120        # km/h
CONGESTION_THRESHOLD = 250      # vehicle_count
SUMMARY_INTERVAL = 60           # seconds between location summaries
ANOMALY_CHANCE = 0.10           # 10% of events are forced anomalies


class TrafficSimulator:
    def __init__(self):
        self.producer = None
        self.sql_conn = None

        # Rolling stats used to build the 60-second location summaries.
        # Reset after each summary flush.
        self.stats = defaultdict(lambda: {"speed_sum": 0.0,
                                          "vehicle_sum": 0,
                                          "count": 0,
                                          "alerts": 0})
        self.last_summary_time = time.time()

    # ------------------------------------------------------------------ #
    # Connection management (with reconnection)
    # ------------------------------------------------------------------ #
    def connect_event_hub(self):
        """(Re)create the Event Hub producer client."""
        try:
            if self.producer is not None:
                try:
                    self.producer.close()
                except Exception:
                    pass
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=EVENT_HUB_CONNECTION_STR,
                eventhub_name=EVENT_HUB_NAME,
            )
            print("[CONN] Event Hub producer connected.")
        except Exception as exc:
            print(f"[CONN][ERROR] Event Hub connect failed: {exc}")
            self.producer = None

    def connect_sql(self):
        """(Re)create the SQL connection."""
        try:
            if self.sql_conn is not None:
                try:
                    self.sql_conn.close()
                except Exception:
                    pass
            self.sql_conn = pyodbc.connect(get_sql_connection_string())
            print("[CONN] Azure SQL connected.")
        except Exception as exc:
            print(f"[CONN][ERROR] SQL connect failed: {exc}")
            self.sql_conn = None

    def ensure_connections(self):
        if self.producer is None:
            self.connect_event_hub()
        if self.sql_conn is None:
            self.connect_sql()

    # ------------------------------------------------------------------ #
    # Event generation
    # ------------------------------------------------------------------ #
    def generate_event(self):
        location = fake.random_element(LOCATIONS)

        # 10% of the time, force an anomaly (high speed OR heavy congestion).
        if fake.pyfloat(min_value=0, max_value=1) < ANOMALY_CHANCE:
            if fake.boolean():
                speed = round(fake.pyfloat(min_value=121, max_value=160), 1)
                vehicle_count = fake.random_int(50, 400)
            else:
                speed = round(fake.pyfloat(min_value=20, max_value=160), 1)
                vehicle_count = fake.random_int(251, 400)
        else:
            speed = round(fake.pyfloat(min_value=20, max_value=160), 1)
            vehicle_count = fake.random_int(50, 400)

        return {
            "vehicle_id": str(uuid.uuid4()),
            "speed": speed,
            "vehicle_count": vehicle_count,
            "latitude": location["lat"],
            "longitude": location["lon"],
            "location_name": location["name"],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def detect_anomaly(self, event):
        """Return (anomaly_type, severity) or (None, None) if normal."""
        speed = event["speed"]
        count = event["vehicle_count"]

        speeding = speed > SPEEDING_THRESHOLD
        congestion = count > CONGESTION_THRESHOLD

        if speeding and congestion:
            return "ACCIDENT_RISK", "HIGH"
        if speeding:
            severity = "HIGH" if speed > 145 else "MEDIUM"
            return "SPEEDING", severity
        if congestion:
            severity = "HIGH" if count > 350 else "MEDIUM"
            return "CONGESTION", severity
        return None, None

    # ------------------------------------------------------------------ #
    # Sending / writing
    # ------------------------------------------------------------------ #
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
            self.producer = None  # force reconnect next cycle
            return False

    def write_traffic_event(self, event):
        try:
            if self.sql_conn is None:
                self.connect_sql()
            if self.sql_conn is None:
                return False
            cursor = self.sql_conn.cursor()
            cursor.execute(
                """
                INSERT INTO traffic_events
                    (vehicle_id, speed, vehicle_count, latitude, longitude,
                     location_name, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                event["vehicle_id"], event["speed"], event["vehicle_count"],
                event["latitude"], event["longitude"], event["location_name"],
                event["timestamp"],
            )
            self.sql_conn.commit()
            return True
        except Exception as exc:
            print(f"[SQL][ERROR] insert traffic_events failed, will reconnect: {exc}")
            self.sql_conn = None
            return False

    def write_anomaly(self, event, anomaly_type, severity):
        try:
            if self.sql_conn is None:
                self.connect_sql()
            if self.sql_conn is None:
                return False
            cursor = self.sql_conn.cursor()
            cursor.execute(
                """
                INSERT INTO anomaly_log
                    (vehicle_id, anomaly_type, speed, vehicle_count,
                     location_name, severity, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                event["vehicle_id"], anomaly_type, event["speed"],
                event["vehicle_count"], event["location_name"], severity,
                event["timestamp"],
            )
            self.sql_conn.commit()
            return True
        except Exception as exc:
            print(f"[SQL][ERROR] insert anomaly_log failed, will reconnect: {exc}")
            self.sql_conn = None
            return False

    def write_location_summaries(self):
        """Flush rolling per-location stats into location_summary."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            if self.sql_conn is None:
                self.connect_sql()
            if self.sql_conn is None:
                return
            cursor = self.sql_conn.cursor()
            for location, data in self.stats.items():
                if data["count"] == 0:
                    continue
                avg_speed = round(data["speed_sum"] / data["count"], 1)
                cursor.execute(
                    """
                    INSERT INTO location_summary
                        (location_name, avg_speed, total_vehicles,
                         alert_count, summary_time)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    location, avg_speed, data["vehicle_sum"],
                    data["alerts"], now,
                )
                print(f"[SUMMARY] {location}: avg_speed={avg_speed} km/h, "
                      f"vehicles={data['vehicle_sum']}, alerts={data['alerts']}")
            self.sql_conn.commit()
        except Exception as exc:
            print(f"[SQL][ERROR] insert location_summary failed, will reconnect: {exc}")
            self.sql_conn = None
        finally:
            # Reset stats for the next 60-second window regardless of outcome.
            self.stats.clear()
            self.last_summary_time = time.time()

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #
    def run(self):
        check_required_env()
        print("=" * 60)
        print(" Real-Time Traffic Analytics - Simulator")
        print(" Generating 1 event/sec. Press Ctrl+C to stop.")
        print("=" * 60 + "\n")

        self.ensure_connections()

        try:
            while True:
                event = self.generate_event()

                # 1) Send to Event Hub
                hub_ok = self.send_to_event_hub(event)

                # 2) Write to traffic_events
                sql_ok = self.write_traffic_event(event)

                # 3) Detect + log anomalies
                anomaly_type, severity = self.detect_anomaly(event)
                is_alert = anomaly_type is not None
                if is_alert:
                    self.write_anomaly(event, anomaly_type, severity)

                # 4) Update rolling stats for the location summary
                s = self.stats[event["location_name"]]
                s["speed_sum"] += event["speed"]
                s["vehicle_sum"] += event["vehicle_count"]
                s["count"] += 1
                if is_alert:
                    s["alerts"] += 1

                # 5) Console output
                hub_flag = "HUB:OK " if hub_ok else "HUB:-- "
                sql_flag = "SQL:OK " if sql_ok else "SQL:-- "
                if is_alert:
                    print(f"[EVENT] {hub_flag}{sql_flag}| {event['location_name']:<20} "
                          f"speed={event['speed']:>5} km/h count={event['vehicle_count']:>3} "
                          f"  ** ANOMALY: {anomaly_type} ({severity}) **")
                else:
                    print(f"[EVENT] {hub_flag}{sql_flag}| {event['location_name']:<20} "
                          f"speed={event['speed']:>5} km/h count={event['vehicle_count']:>3}")

                # 6) Every 60 seconds, flush location summaries
                if time.time() - self.last_summary_time >= SUMMARY_INTERVAL:
                    print("\n--- Writing 60-second location summaries ---")
                    self.write_location_summaries()
                    print("--- Summaries written ---\n")

                time.sleep(1)

        except KeyboardInterrupt:
            print("\n\nStopping simulator (Ctrl+C received)...")
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
        print("Simulator stopped. Goodbye.")


if __name__ == "__main__":
    TrafficSimulator().run()
