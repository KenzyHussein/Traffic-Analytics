"""
warehouse_setup.py  (PHASE 2)
=============================
Creates the star-schema data warehouse in Azure SQL.

Each table is DROPPED IF EXISTS and then re-created, so this script can be
run repeatedly to get a clean warehouse. Tables are dropped in dependency
order (facts before dimensions).

    dim_location, dim_weather, dim_time   (dimensions)
    fact_traffic, fact_anomalies          (facts)

Run:  python warehouse_setup.py
"""

import sys

import pyodbc

from config import check_required_env, get_sql_connection_string

# Drop facts first (they logically reference the dimensions).
DROP_ORDER = [
    "fact_anomalies",
    "fact_traffic",
    "dim_time",
    "dim_weather",
    "dim_location",
]

# (table_name, CREATE statement) in creation order (dimensions first).
CREATE_STATEMENTS = [
    (
        "dim_location",
        """
        CREATE TABLE dim_location (
            location_id   INT PRIMARY KEY,
            location_name VARCHAR(100),
            city          VARCHAR(50),
            region        VARCHAR(50)
        )
        """,
    ),
    (
        "dim_weather",
        """
        CREATE TABLE dim_weather (
            weather_id        INT IDENTITY(1,1) PRIMARY KEY,
            weather_condition VARCHAR(50),
            description       VARCHAR(200)
        )
        """,
    ),
    (
        "dim_time",
        """
        CREATE TABLE dim_time (
            time_id        INT IDENTITY(1,1) PRIMARY KEY,
            full_timestamp DATETIME,
            hour           INT,
            day_of_week    VARCHAR(20),
            month          INT,
            year           INT,
            is_peak_hour   BIT
        )
        """,
    ),
    (
        "fact_traffic",
        """
        CREATE TABLE fact_traffic (
            record_id           INT IDENTITY(1,1) PRIMARY KEY,
            time_id             INT,
            location_id         INT,
            weather_id          INT,
            traffic_volume      INT,
            avg_vehicle_speed   FLOAT,
            vehicle_count_cars  INT,
            vehicle_count_trucks INT,
            vehicle_count_bikes INT,
            total_vehicles      INT,
            temperature         FLOAT,
            humidity            FLOAT,
            signal_status       VARCHAR(20),
            speed_category      VARCHAR(20),
            created_at          DATETIME DEFAULT GETDATE()
        )
        """,
    ),
    (
        "fact_anomalies",
        """
        CREATE TABLE fact_anomalies (
            anomaly_id        INT IDENTITY(1,1) PRIMARY KEY,
            record_id         INT,
            location_id       INT,
            anomaly_type      VARCHAR(50),
            severity          VARCHAR(20),
            avg_vehicle_speed FLOAT,
            traffic_volume    INT,
            accident_reported INT,
            detected_at       DATETIME,
            created_at        DATETIME DEFAULT GETDATE()
        )
        """,
    ),
]


def main() -> None:
    check_required_env()

    print("Connecting to Azure SQL Database...")
    try:
        conn = pyodbc.connect(get_sql_connection_string())
    except pyodbc.Error as exc:
        print(f"FAILED to connect: {exc}")
        sys.exit(1)

    cursor = conn.cursor()
    print("Connected.\n")

    # 1) Drop existing tables.
    print("Dropping existing tables (if any)...")
    for table in DROP_ORDER:
        try:
            cursor.execute(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}")
            conn.commit()
            print(f"   [drop] {table}")
        except pyodbc.Error as exc:
            print(f"   [ERROR] dropping {table}: {exc}")

    # 2) Create tables.
    print("\nCreating warehouse tables...")
    for table_name, create_sql in CREATE_STATEMENTS:
        try:
            cursor.execute(create_sql)
            conn.commit()
            print(f"   [OK] '{table_name}' created successfully.")
        except pyodbc.Error as exc:
            print(f"   [ERROR] creating {table_name}: {exc}")

    cursor.close()
    conn.close()
    print("\nWarehouse setup complete.")


if __name__ == "__main__":
    main()
