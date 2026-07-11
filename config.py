"""
Shared configuration and connection helpers.

Loads credentials from the .env file and builds the connection objects
that db_setup.py, test_connections.py and simulator.py all reuse.
"""

import os
import sys

from dotenv import load_dotenv

# Load variables from .env into the process environment.
load_dotenv()

# ---- Event Hub ----
EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# ---- Azure SQL ----
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

# The ODBC driver name. "ODBC Driver 18 for SQL Server" is the current
# recommended driver for Azure SQL. If you have an older one installed,
# change this to "ODBC Driver 17 for SQL Server".
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")


def get_sql_connection_string() -> str:
    """Build the pyodbc connection string for Azure SQL Database."""
    return (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )


def check_required_env() -> None:
    """Fail fast with a clear message if any credential is missing."""
    required = {
        "EVENT_HUB_CONNECTION_STR": EVENT_HUB_CONNECTION_STR,
        "EVENT_HUB_NAME": EVENT_HUB_NAME,
        "SQL_SERVER": SQL_SERVER,
        "SQL_DATABASE": SQL_DATABASE,
        "SQL_USERNAME": SQL_USERNAME,
        "SQL_PASSWORD": SQL_PASSWORD,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        print("ERROR: The following variables are missing from your .env file:")
        for name in missing:
            print(f"   - {name}")
        sys.exit(1)
