# Real-Time Traffic Analytics Using Azure Stream Analytics

A Python data pipeline that simulates live vehicle traffic for five Egyptian
locations, streams each event into **Azure Event Hubs**, and stores events,
anomalies, and rolling summaries in an **Azure SQL Database**. Built as a
graduation project for the **Digital Egypt Pioneers Initiative (AI & Data
Science Track)**.

```
                +-------------------+
                |   simulator.py    |
                | (data generator)  |
                +---------+---------+
                          |
          +---------------+----------------+
          |                                |
          v                                v
  +----------------+              +-------------------+
  | Azure Event Hub|              |  Azure SQL DB     |
  | traffic-events |              |  traffic_events   |
  +-------+--------+              |  anomaly_log      |
          |                       |  location_summary |
          v                       +-------------------+
  Azure Stream Analytics
  (real-time queries / dashboards)
```

## What it does

- Generates **one vehicle event every second** for 5 locations:
  Ring Road Cairo, Corniche Alexandria, Suez Road, October Bridge, Tahrir Square.
- Each event has a UUID vehicle id, speed (20–160 km/h), vehicle count
  (50–400), latitude/longitude, location name, and a timestamp.
- ~10% of events are **anomalies** (speed > 120 km/h or vehicle count > 250).
- Sends every event as JSON to **Azure Event Hubs**.
- Writes every event to the **`traffic_events`** table.
- Detects anomalies and writes them to **`anomaly_log`**
  (`SPEEDING`, `CONGESTION`, or `ACCIDENT_RISK`, with LOW/MEDIUM/HIGH severity).
- Every 60 seconds writes a per-location aggregate to **`location_summary`**.
- Full error handling with automatic reconnection, real-time console output,
  and runs until you press `Ctrl+C`.

## Prerequisites

- **Python 3.10+** and **pip**
- **Microsoft ODBC Driver 18 for SQL Server** (required by `pyodbc`)
  - Windows: [Download the ODBC Driver 18 installer](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
  - If you already have Driver 17, set `ODBC_DRIVER=ODBC Driver 17 for SQL Server` in your `.env`.
- An Azure account with:
  - An Event Hubs namespace + an event hub named `traffic-events`
  - An Azure SQL Database
  - Your client IP added to the **Azure SQL Server firewall** rules

## Project files

| File                  | Purpose                                              |
|-----------------------|------------------------------------------------------|
| `requirements.txt`    | Python dependencies                                  |
| `.env`                | Your Azure credentials (never commit this)           |
| `config.py`           | Loads `.env` and builds connections (shared helper)  |
| `db_setup.py`         | Creates the 3 SQL tables                             |
| `test_connections.py` | Tests Event Hub + SQL connections                    |
| `simulator.py`        | The main data generator                              |

## Installation

```bash
# 1. (Recommended) create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate          # Windows PowerShell
# source .venv/bin/activate     # macOS / Linux

# 2. Install the dependencies
pip install -r requirements.txt
```

Make sure your `.env` file contains your real credentials before continuing.

## How to run (in order)

### Step 1 — Create the database tables

```bash
python db_setup.py
```

Expected output:

```
Connecting to Azure SQL Database...
Connected. Creating tables (if they do not already exist)...

   [OK] 'traffic_events' is ready.
   [OK] 'anomaly_log' is ready.
   [OK] 'location_summary' is ready.

Database setup complete.
```

### Step 2 — Test the connections

```bash
python test_connections.py
```

Expected output:

```
=======================================================
 Connection Test - Real-Time Traffic Analytics
=======================================================

Testing Azure SQL Database connection...
   [SUCCESS] Connected to Azure SQL Database.

Testing Azure Event Hub connection...
   [SUCCESS] Connected to Event Hub 'traffic-events' (2 partition(s)).

-------------------------------------------------------
 Azure SQL Database : SUCCESS
 Azure Event Hub    : SUCCESS
-------------------------------------------------------

All connections OK. You can now run: python simulator.py
```

### Step 3 — Run the simulator

```bash
python simulator.py
```

Press `Ctrl+C` to stop.

## What the console output looks like

```
============================================================
 Real-Time Traffic Analytics - Simulator
 Generating 1 event/sec. Press Ctrl+C to stop.
============================================================

[CONN] Event Hub producer connected.
[CONN] Azure SQL connected.
[EVENT] HUB:OK SQL:OK | Ring Road Cairo      speed= 88.4 km/h count=142
[EVENT] HUB:OK SQL:OK | Suez Road            speed=134.1 km/h count= 97   ** ANOMALY: SPEEDING (MEDIUM) **
[EVENT] HUB:OK SQL:OK | Corniche Alexandria  speed= 41.7 km/h count=301   ** ANOMALY: CONGESTION (MEDIUM) **
[EVENT] HUB:OK SQL:OK | October Bridge       speed= 72.0 km/h count=188
...

--- Writing 60-second location summaries ---
[SUMMARY] Ring Road Cairo: avg_speed=86.3 km/h, vehicles=8421, alerts=5
[SUMMARY] Suez Road: avg_speed=91.2 km/h, vehicles=7110, alerts=4
--- Summaries written ---
```

- `HUB:OK` = sent to Event Hub. `SQL:OK` = written to the database.
- If a connection drops, you'll see a `[...][ERROR] ... will reconnect`
  line and the pipeline automatically reconnects on the next cycle.

## How to verify data is appearing in Azure SQL

Use the **Azure Portal → SQL Database → Query editor**, or
**Azure Data Studio / SSMS**, and run:

```sql
-- Latest raw events
SELECT TOP 20 * FROM traffic_events ORDER BY created_at DESC;

-- Latest detected anomalies
SELECT TOP 20 * FROM anomaly_log ORDER BY created_at DESC;

-- Per-location summaries (written every 60 seconds)
SELECT TOP 20 * FROM location_summary ORDER BY summary_time DESC;

-- Row counts
SELECT
    (SELECT COUNT(*) FROM traffic_events)   AS traffic_events,
    (SELECT COUNT(*) FROM anomaly_log)      AS anomaly_log,
    (SELECT COUNT(*) FROM location_summary) AS location_summary;
```

Row counts in `traffic_events` should grow by roughly 60 per minute while the
simulator is running.

## Troubleshooting

| Problem | Fix |

| `Can't open lib 'ODBC Driver 18 for SQL Server'` | Install the ODBC driver, or set `ODBC_DRIVER` in `.env` to your installed version (e.g. 17). |
| SQL `Cannot open server ... requested by the login` / timeout | Add your client IP to the Azure SQL Server firewall rules. |
| Event Hub `Unauthorized` / authentication error | Check `EVENT_HUB_CONNECTION_STR` and `EVENT_HUB_NAME` in `.env`. |
| `variables are missing from your .env` | Make sure `.env` exists in the project folder and is filled in. |

## Security note

The `.env` file holds live credentials and is excluded from git via
`.gitignore`. Never commit it or paste these credentials anywhere public.
Rotate the SQL password and Event Hub key if they have ever been exposed.
