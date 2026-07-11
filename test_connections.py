"""
test_connections.py
-------------------
Quick health-check. Tests BOTH connections and prints SUCCESS or FAILURE
for each one. Run this BEFORE running simulator.py so you know your
credentials and network access are working.
"""

import sys

import pyodbc
from azure.eventhub import EventHubProducerClient

from config import (
    EVENT_HUB_CONNECTION_STR,
    EVENT_HUB_NAME,
    check_required_env,
    get_sql_connection_string,
)


def test_sql() -> bool:
    print("Testing Azure SQL Database connection...")
    try:
        conn = pyodbc.connect(get_sql_connection_string())
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print("   [SUCCESS] Connected to Azure SQL Database.\n")
        return True
    except Exception as exc:
        print("   [FAILURE] Could not connect to Azure SQL Database.")
        print(f"      Details: {exc}\n")
        return False


def test_event_hub() -> bool:
    print("Testing Azure Event Hub connection...")
    producer = None
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STR,
            eventhub_name=EVENT_HUB_NAME,
        )
        # Asking for partition ids forces a real connection to the service.
        partition_ids = producer.get_partition_ids()
        print(f"   [SUCCESS] Connected to Event Hub '{EVENT_HUB_NAME}' "
              f"({len(partition_ids)} partition(s)).\n")
        return True
    except Exception as exc:
        print("   [FAILURE] Could not connect to Azure Event Hub.")
        print(f"      Details: {exc}\n")
        return False
    finally:
        if producer is not None:
            producer.close()


def main() -> None:
    check_required_env()
    print("=" * 55)
    print(" Connection Test - Real-Time Traffic Analytics")
    print("=" * 55 + "\n")

    sql_ok = test_sql()
    hub_ok = test_event_hub()

    print("-" * 55)
    print(f" Azure SQL Database : {'SUCCESS' if sql_ok else 'FAILURE'}")
    print(f" Azure Event Hub    : {'SUCCESS' if hub_ok else 'FAILURE'}")
    print("-" * 55)

    if sql_ok and hub_ok:
        print("\nAll connections OK. You can now run: python simulator.py")
        sys.exit(0)
    else:
        print("\nFix the failing connection(s) before running simulator.py.")
        sys.exit(1)


if __name__ == "__main__":
    main()
