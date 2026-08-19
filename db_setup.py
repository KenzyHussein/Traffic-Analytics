"""
db_setup.py
---------
Connects to the Azure SQL Database and creates the three tables used by the
Real-Time Traffic Analytics project, if they do not already exist:

    1. traffic_events    - every vehicle reading
    2. anomaly_log       - detected anomalies (speeding / congestion / accident risk)
    3. location_summary  - rolling per-location aggregates

Run this ONCE before running simulator.py.
"""

import sys

import pyodbc

from config import check_required_env, get_sql_connection_string

# Each entry: (table_name, CREATE TABLE statement).
# We guard every CREATE with a check against sys.tables so the script is safe
# to run multiple times.
TABLES = [
    (
        "traffic_events",
        """
        CREATE TABLE traffic_events (
            event_id      INT IDENTITY(1,1) PRIMARY KEY,
            vehicle_id    VARCHAR(50),
            speed         FLOAT,
            vehicle_count INT,
            latitude      FLOAT,
            longitude     FLOAT,
            location_name VARCHAR(100),
            timestamp     DATETIME,
            created_at    DATETIME DEFAULT GETDATE()
        )
        """,
    ),
    (
        "anomaly_log",
        """
        CREATE TABLE anomaly_log (
            anomaly_id    INT IDENTITY(1,1) PRIMARY KEY,
            vehicle_id    VARCHAR(50),
            anomaly_type  VARCHAR(50),
            speed         FLOAT,
            vehicle_count INT,
            location_name VARCHAR(100),
            severity      VARCHAR(20),
            timestamp     DATETIME,
            created_at    DATETIME DEFAULT GETDATE()
        )
        """,
    ),
    (
        "location_summary",
        """
        CREATE TABLE location_summary (
            summary_id     INT IDENTITY(1,1) PRIMARY KEY,
            location_name  VARCHAR(100),
            avg_speed      FLOAT,
            total_vehicles INT,
            alert_count    INT,
            summary_time   DATETIME
        )
        """,
    ),
]


def create_tables() -> None:
    check_required_env()

    print("Connecting to Azure SQL Database...")
    try:
        conn = pyodbc.connect(get_sql_connection_string())
    except pyodbc.Error as exc:
        print("FAILED to connect to Azure SQL Database.")
        print(f"   Details: {exc}")
        sys.exit(1)

    print("Connected. Creating tables (if they do not already exist)...\n")
    cursor = conn.cursor()

    for table_name, create_sql in TABLES:
        try:
            # Only create the table when it is missing.
            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT * FROM sys.tables WHERE name = ?
                )
                BEGIN
                    EXEC(?)
                END
                """,
                table_name,
                create_sql,
            )
            conn.commit()
            print(f"   [OK] '{table_name}' is ready.")
        except pyodbc.Error as exc:
            print(f"   [ERROR] Could not create '{table_name}': {exc}")

    cursor.close()
    conn.close()
    print("\nDatabase setup complete.")


if __name__ == "__main__":
    create_tables()
