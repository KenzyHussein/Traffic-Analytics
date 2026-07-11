"""
dashboard.py
============
Real-Time Traffic Analytics Dashboard (Streamlit + Plotly).

Digital Egypt Pioneers Initiative - Live Monitoring System.

This single file is organised in clearly labelled sections:

  SECTION 1  - Imports & configuration (page setup, dark theme CSS)
  SECTION 2  - Database connection + warehouse query helpers
  SECTION 3  - Sidebar (clock, LIVE badge, totals, manual refresh)
  SECTION 4  - Header (title + subtitle)
  SECTION 5  - Top row: 5 KPI metric cards
  SECTION 6  - Bar charts: vehicle count / avg speed by location
  SECTION 7  - Alert log table (coloured severity)
  SECTION 8  - Traffic volume distribution histogram
  SECTION 8b - Anomaly type breakdown (donut)
  SECTION 8c - Weather distribution (bar) + signal status (pie)
  SECTION 8d - Peak vs Off-Peak traffic (bar)
  SECTION 9  - Auto-refresh every 5 seconds with a countdown timer

Data source: the star-schema warehouse (fact_traffic, fact_anomalies,
dim_location, dim_weather) built by warehouse_setup.py + load_warehouse.py.

Run with:  streamlit run dashboard.py   (or: python -m streamlit run dashboard.py)
"""

# ======================================================================
# SECTION 1 - IMPORTS & CONFIGURATION
# ======================================================================
import os
import time
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import pyodbc
import streamlit as st
from dotenv import load_dotenv

# Load credentials from the .env file in the project folder.
load_dotenv()

# Database credentials (fall back to the known values if .env is missing).
SQL_SERVER = os.getenv("SQL_SERVER", "sql-traffic-marwan.database.windows.net")
SQL_DATABASE = os.getenv("SQL_DATABASE", "db-traffic")
SQL_USERNAME = os.getenv("SQL_USERNAME", "CloudSAeace38df")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "Traffic@2026")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

# How often the dashboard refreshes itself.
REFRESH_SECONDS = 5

# Theme colours.
COLOR_BG = "#0e1117"
COLOR_CARD = "#1e2130"
COLOR_ACCENT = "#00d4ff"
COLOR_GREEN = "#21c45d"
COLOR_AMBER = "#f5a524"
COLOR_RED = "#ef4444"

# --- Page configuration ---
st.set_page_config(
    page_title="Real-Time Traffic Analytics Dashboard",
    page_icon="🚦",
    layout="wide",
)

# --- Dark theme via injected CSS ---
st.markdown(
    f"""
    <style>
        .stApp {{
            background-color: {COLOR_BG};
            color: white;
        }}
        section[data-testid="stSidebar"] {{
            background-color: {COLOR_CARD};
        }}
        /* KPI metric cards */
        div[data-testid="stMetric"] {{
            background-color: {COLOR_CARD};
            border: 1px solid #2a2e3e;
            border-radius: 12px;
            padding: 18px 16px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.35);
        }}
        div[data-testid="stMetricValue"] {{
            color: {COLOR_ACCENT};
            font-weight: 700;
        }}
        div[data-testid="stMetricLabel"] {{
            color: #c7c9d1;
        }}
        h1, h2, h3, h4, h5, h6 {{ color: white; }}
        .subtitle {{ color: {COLOR_ACCENT}; font-size: 1.05rem; margin-top: -8px; }}
        .live-badge {{
            background-color: {COLOR_GREEN};
            color: white;
            padding: 4px 14px;
            border-radius: 20px;
            font-weight: 700;
            letter-spacing: 1px;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ======================================================================
# SECTION 2 - DATABASE CONNECTION + QUERY HELPERS
# ======================================================================
def get_connection():
    """Open a fresh pyodbc connection. Raises on failure (handled by caller)."""
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=15;"
    )
    return pyodbc.connect(conn_str)


def run_query(conn, sql):
    """Run a SQL query and return a DataFrame (empty DataFrame on error)."""
    try:
        return pd.read_sql(sql, conn)
    except Exception as exc:
        st.warning(f"Query failed: {exc}")
        return pd.DataFrame()


# This dashboard reads the star-schema data warehouse:
#   fact_traffic, fact_anomalies, dim_location, dim_weather, dim_time.
# KPI cards and the location/volume charts use a rolling 60-minute window on
# created_at (GETDATE() = local SQL server time). Because the historical
# 2000 rows are loaded with a recent created_at, the dashboard shows data
# immediately even before the streaming simulator runs. Breakdown charts
# (anomaly / weather / signal / peak) query the full fact tables.

QUERIES = {
    # --- KPIs (last 60 minutes, by created_at) ---
    # NOTE: the total_vehicles KPI intentionally does NOT inner-join dim_time.
    # Live streamed rows are inserted with time_id = NULL, and an inner join
    # to dim_time would silently drop them from the total.
    "kpi_total_vehicles": """
        SELECT ISNULL(SUM(total_vehicles), 0) AS total
        FROM fact_traffic
        WHERE created_at >= DATEADD(MINUTE, -60, GETDATE())
    """,
    "kpi_avg_speed": """
        SELECT ISNULL(AVG(avg_vehicle_speed), 0) AS avg_speed
        FROM fact_traffic
        WHERE created_at >= DATEADD(MINUTE, -60, GETDATE())
    """,
    "kpi_active_anomalies": """
        SELECT COUNT(*) AS c
        FROM fact_anomalies
        WHERE created_at >= DATEADD(MINUTE, -60, GETDATE())
    """,
    # Most congested = location of the single highest traffic_volume reading.
    "kpi_congested": """
        SELECT TOP 1 l.location_name
        FROM fact_traffic f
        JOIN dim_location l ON f.location_id = l.location_id
        WHERE f.created_at >= DATEADD(MINUTE, -60, GETDATE())
        ORDER BY f.traffic_volume DESC
    """,
    # NEW KPI: accidents (anomaly_type = 'ACCIDENT') in the last 60 minutes
    "kpi_accidents": """
        SELECT COUNT(*) AS c
        FROM fact_anomalies
        WHERE anomaly_type = 'ACCIDENT'
          AND created_at >= DATEADD(MINUTE, -60, GETDATE())
    """,
    # --- Location charts (last 60 minutes) ---
    "count_by_location": """
        SELECT l.location_name, SUM(f.total_vehicles) AS total_count
        FROM fact_traffic f
        JOIN dim_location l ON f.location_id = l.location_id
        WHERE f.created_at >= DATEADD(MINUTE, -60, GETDATE())
        GROUP BY l.location_name
        ORDER BY total_count DESC
    """,
    "speed_by_location": """
        SELECT l.location_name, AVG(f.avg_vehicle_speed) AS avg_speed
        FROM fact_traffic f
        JOIN dim_location l ON f.location_id = l.location_id
        WHERE f.created_at >= DATEADD(MINUTE, -60, GETDATE())
        GROUP BY l.location_name
        ORDER BY avg_speed DESC
    """,
    # Traffic volume distribution (last 60 minutes)
    "volume_values": """
        SELECT traffic_volume
        FROM fact_traffic
        WHERE created_at >= DATEADD(MINUTE, -60, GETDATE())
    """,
    # --- Alert log (last 20 anomalies) ---
    "alert_log": """
        SELECT TOP 20
            fa.detected_at AS [Time], l.location_name AS [Location],
            fa.anomaly_type AS [Type], fa.avg_vehicle_speed AS [Speed],
            fa.traffic_volume AS [Volume], fa.severity AS [Severity],
            fa.accident_reported AS [Accident]
        FROM fact_anomalies fa
        JOIN dim_location l ON fa.location_id = l.location_id
        ORDER BY fa.detected_at DESC
    """,
    # --- Anomaly type breakdown (all time) ---
    "anomaly_breakdown": """
        SELECT anomaly_type, COUNT(*) AS total
        FROM fact_anomalies
        GROUP BY anomaly_type
    """,
    # Weather condition distribution (all time)
    "weather_distribution": """
        SELECT w.weather_condition, COUNT(*) AS total
        FROM fact_traffic f
        JOIN dim_weather w ON f.weather_id = w.weather_id
        GROUP BY w.weather_condition
        ORDER BY total DESC
    """,
    # Signal status distribution (all time)
    "signal_distribution": """
        SELECT signal_status, COUNT(*) AS total
        FROM fact_traffic
        GROUP BY signal_status
    """,
    # NEW: Peak vs Off-Peak traffic (all time, needs dim_time)
    "peak_offpeak": """
        SELECT
            CASE WHEN t.is_peak_hour = 1 THEN 'Peak Hour' ELSE 'Off-Peak' END AS period,
            COUNT(*) AS records,
            AVG(f.traffic_volume) AS avg_volume
        FROM fact_traffic f
        JOIN dim_time t ON f.time_id = t.time_id
        GROUP BY t.is_peak_hour
    """,
    # --- Sidebar totals ---
    "total_records": "SELECT COUNT(*) AS c FROM fact_traffic",
    "total_anomalies": "SELECT COUNT(*) AS c FROM fact_anomalies",
    "total_accidents": """
        SELECT COUNT(*) AS c FROM fact_anomalies WHERE anomaly_type = 'ACCIDENT'
    """,
}


def scalar(df, column, default=0):
    """Safely pull the first value of a column from a DataFrame."""
    if df is None or df.empty or column not in df.columns:
        return default
    value = df.iloc[0][column]
    return default if pd.isna(value) else value


# --- Open the connection ONCE per run, with graceful error handling ---
connection = None
connection_error = None
try:
    connection = get_connection()
except Exception as exc:
    connection_error = str(exc)


# ======================================================================
# SECTION 3 - SIDEBAR
# ======================================================================
with st.sidebar:
    st.markdown("### Control Panel")
    st.write(f"**Current time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Pipeline status badge
    if connection is not None:
        st.markdown(
            "**Pipeline status:** <span class='live-badge'>● LIVE</span>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "**Pipeline status:** <span style='color:#ef4444;font-weight:700;'>● OFFLINE</span>",
            unsafe_allow_html=True,
        )

    # Totals
    if connection is not None:
        total_records = int(scalar(run_query(connection, QUERIES["total_records"]), "c"))
        total_anomalies = int(scalar(run_query(connection, QUERIES["total_anomalies"]), "c"))
        total_accidents = int(scalar(run_query(connection, QUERIES["total_accidents"]), "c"))
    else:
        total_records = total_anomalies = total_accidents = 0

    st.metric("Total records (fact_traffic)", f"{total_records:,}")
    st.metric("Total anomalies (fact_anomalies)", f"{total_anomalies:,}")
    st.metric("Accidents", f"{total_accidents:,}")

    st.markdown(
        "**Data source:** Kaggle Smart Traffic Dataset (2000 rows)"
    )

    # Manual refresh button
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.rerun()


# ======================================================================
# SECTION 4 - HEADER
# ======================================================================
st.title("Real-Time Traffic Analytics Dashboard")
st.markdown(
    "<p class='subtitle'>Digital Egypt Pioneers Initiative — Kaggle Dataset | Live Streaming</p>",
    unsafe_allow_html=True,
)

# If the database is unreachable, show a red error and stop (no crash).
if connection is None:
    st.error(
        "❌ Could not connect to the Azure SQL Database.\n\n"
        f"Details: {connection_error}\n\n"
        "Check your credentials in .env, the ODBC driver, and that your IP "
        "is allowed in the Azure SQL firewall. The dashboard will keep retrying."
    )
    # Still auto-refresh so it recovers when the DB comes back.
    time.sleep(REFRESH_SECONDS)
    st.rerun()
    st.stop()


# ======================================================================
# SECTION 5 - TOP ROW: 5 KPI METRIC CARDS
# ======================================================================
k1, k2, k3, k4, k5 = st.columns(5)

total_vehicles = int(scalar(run_query(connection, QUERIES["kpi_total_vehicles"]), "total"))
avg_speed_now = float(scalar(run_query(connection, QUERIES["kpi_avg_speed"]), "avg_speed"))
active_anomalies = int(scalar(run_query(connection, QUERIES["kpi_active_anomalies"]), "c"))
accidents_today = int(scalar(run_query(connection, QUERIES["kpi_accidents"]), "c"))

congested_df = run_query(connection, QUERIES["kpi_congested"])
congested_location = scalar(congested_df, "location_name", default="—")

k1.metric("Total Vehicles (60 min)", f"{total_vehicles:,}")
k2.metric("Avg Speed (60 min, km/h)", f"{avg_speed_now:.1f}")
k3.metric("Active Anomalies (60 min)", f"{active_anomalies}")
k4.metric("Accidents Reported (60 min)", f"{accidents_today}")
k5.metric("Most Congested", congested_location)


# ======================================================================
# SECTION 6 - MIDDLE ROW: 2 BAR CHARTS
# ======================================================================
def styled_layout(fig, title):
    """Apply the dark theme to a Plotly figure."""
    fig.update_layout(
        title=title,
        paper_bgcolor=COLOR_CARD,
        plot_bgcolor=COLOR_CARD,
        font_color="white",
        title_font_color=COLOR_ACCENT,
        margin=dict(l=10, r=10, t=50, b=10),
        height=360,
        xaxis=dict(gridcolor="#2a2e3e"),
        yaxis=dict(gridcolor="#2a2e3e"),
    )
    return fig


c_left, c_right = st.columns(2)

# LEFT: Vehicle Count by Location (RED >5000, AMBER >3000, GREEN otherwise)
with c_left:
    df_count = run_query(connection, QUERIES["count_by_location"])
    if df_count.empty:
        st.info("No traffic data in the last 60 minutes.")
    else:
        colors = [
            COLOR_RED if v > 5000 else COLOR_AMBER if v > 3000 else COLOR_GREEN
            for v in df_count["total_count"]
        ]
        fig = go.Figure(
            go.Bar(
                x=df_count["location_name"],
                y=df_count["total_count"],
                marker_color=colors,
                text=df_count["total_count"],
                textposition="outside",
            )
        )
        st.plotly_chart(
            styled_layout(fig, "Vehicle Count by Location (last 60 min)"),
            use_container_width=True,
        )

# RIGHT: Average Speed by Location (RED >70, AMBER >50, GREEN otherwise)
with c_right:
    df_speed = run_query(connection, QUERIES["speed_by_location"])
    if df_speed.empty:
        st.info("No traffic data in the last 60 minutes.")
    else:
        colors = [
            COLOR_RED if v > 70 else COLOR_AMBER if v > 50 else COLOR_GREEN
            for v in df_speed["avg_speed"]
        ]
        fig = go.Figure(
            go.Bar(
                x=df_speed["location_name"],
                y=df_speed["avg_speed"].round(1),
                marker_color=colors,
                text=df_speed["avg_speed"].round(1),
                textposition="outside",
            )
        )
        st.plotly_chart(
            styled_layout(fig, "Average Speed by Location (last 60 min, km/h)"),
            use_container_width=True,
        )


# ======================================================================
# SECTION 7 & 8 - BOTTOM ROW: ALERT LOG (left) + SPEED HISTOGRAM (right)
# ======================================================================
b_left, b_right = st.columns(2)

# --- SECTION 7: Alert log table with coloured severity ---
with b_left:
    st.subheader("Recent Alerts")
    df_alerts = run_query(connection, QUERIES["alert_log"])
    if df_alerts.empty:
        st.info("No anomalies logged yet.")
    else:
        # Format the columns nicely.
        df_alerts["Time"] = pd.to_datetime(df_alerts["Time"]).dt.strftime("%H:%M:%S")
        df_alerts["Speed"] = df_alerts["Speed"].round(1)
        # Show accident flag as Yes/No instead of 1/0.
        df_alerts["Accident"] = df_alerts["Accident"].map({1: "Yes", 0: "No"}).fillna("No")

        def color_severity(val):
            v = str(val).upper()
            if v == "HIGH":
                return "background-color: #ef4444; color: white;"
            if v == "MEDIUM":
                return "background-color: #f5a524; color: black;"
            if v == "LOW":
                return "background-color: #facc15; color: black;"
            return ""

        styled = df_alerts.style.map(color_severity, subset=["Severity"])
        st.dataframe(styled, use_container_width=True, hide_index=True, height=360)

# --- SECTION 8: Traffic volume distribution histogram (green/amber/red bins) ---
with b_right:
    st.subheader("Traffic Volume Distribution (last 60 min)")
    df_vol = run_query(connection, QUERIES["volume_values"])
    if df_vol.empty:
        st.info("No traffic data in the last 60 minutes.")
    else:
        vol = df_vol["traffic_volume"]
        # Split into three coloured bands and overlay them.
        bands = [
            ("Low (0-250)", vol[vol < 250], COLOR_GREEN),
            ("Medium (250-600)", vol[(vol >= 250) & (vol < 600)], COLOR_AMBER),
            ("High (600+)", vol[vol >= 600], COLOR_RED),
        ]
        fig = go.Figure()
        for name, data, color in bands:
            fig.add_trace(
                go.Histogram(
                    x=data, name=name, marker_color=color, xbins=dict(size=50)
                )
            )
        fig.update_xaxes(title_text="Traffic volume (vehicles)")
        fig.update_yaxes(title_text="Number of records")
        # Apply the shared dark theme first, then override for this chart:
        # drop the in-figure title (the section heading above already labels
        # it) and move the legend to the bottom so it does not overlap.
        fig = styled_layout(fig, "Traffic Volume Distribution")
        fig.update_layout(
            barmode="stack",
            title="",
            margin=dict(l=10, r=10, t=10, b=80),
            legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
        )
        st.plotly_chart(fig, use_container_width=True)


# ======================================================================
# SECTION 8b - ANOMALY TYPE BREAKDOWN (DONUT CHART)
# ======================================================================
st.subheader("Anomaly Type Breakdown")
df_breakdown = run_query(connection, QUERIES["anomaly_breakdown"])
if df_breakdown.empty:
    st.info("No anomalies logged yet.")
else:
    # Fixed colour per anomaly type so the legend is always consistent.
    type_colors = {
        "CONGESTION": COLOR_ACCENT,   # cyan
        "ACCIDENT": COLOR_RED,        # red
        "ACCIDENT_RISK": "#f59e0b",   # orange
        "SPEEDING": COLOR_AMBER,
    }
    labels = df_breakdown["anomaly_type"].tolist()
    values = df_breakdown["total"].tolist()
    colors = [type_colors.get(t, "#9ca3af") for t in labels]

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.55,  # donut
            marker=dict(colors=colors, line=dict(color=COLOR_BG, width=2)),
            textinfo="label+percent",
            insidetextorientation="radial",
        )
    )
    fig.update_layout(
        paper_bgcolor=COLOR_CARD,
        plot_bgcolor=COLOR_CARD,
        font_color="white",
        title_text="Anomaly Types (all time)",
        title_font_color=COLOR_ACCENT,
        legend=dict(orientation="h"),
        margin=dict(l=10, r=10, t=50, b=10),
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)


# ======================================================================
# SECTION 8c - WEATHER DISTRIBUTION (bar) + SIGNAL STATUS (pie)
# ======================================================================
w_left, w_right = st.columns(2)

# Weather condition colours (Windy is present in the real data; Snowy kept
# for completeness). Unknown conditions fall back to grey.
WEATHER_COLORS = {
    "Sunny": "#facc15",     # yellow
    "Rainy": "#3b82f6",     # blue
    "Foggy": "#9ca3af",     # gray
    "Cloudy": "#93c5fd",    # light blue
    "Snowy": "#ffffff",     # white
    "Windy": "#2dd4bf",     # teal
}

# LEFT: Weather Condition Distribution (bar)
with w_left:
    st.subheader("Weather Condition Distribution")
    df_weather = run_query(connection, QUERIES["weather_distribution"])
    if df_weather.empty:
        st.info("No weather data available.")
    else:
        colors = [WEATHER_COLORS.get(w, "#9ca3af") for w in df_weather["weather_condition"]]
        fig = go.Figure(
            go.Bar(
                x=df_weather["weather_condition"],
                y=df_weather["total"],
                marker_color=colors,
                text=df_weather["total"],
                textposition="outside",
            )
        )
        st.plotly_chart(
            styled_layout(fig, "Records by Weather Condition"),
            use_container_width=True,
        )

# RIGHT: Signal Status Distribution (pie)
with w_right:
    st.subheader("Signal Status Distribution")
    df_signal = run_query(connection, QUERIES["signal_distribution"])
    if df_signal.empty:
        st.info("No signal data available.")
    else:
        signal_colors = {"Red": COLOR_RED, "Green": COLOR_GREEN, "Yellow": "#facc15"}
        labels = df_signal["signal_status"].tolist()
        colors = [signal_colors.get(s, "#9ca3af") for s in labels]
        fig = go.Figure(
            go.Pie(
                labels=labels,
                values=df_signal["total"].tolist(),
                marker=dict(colors=colors, line=dict(color=COLOR_BG, width=2)),
                textinfo="label+percent",
            )
        )
        fig.update_layout(
            paper_bgcolor=COLOR_CARD,
            plot_bgcolor=COLOR_CARD,
            font_color="white",
            title_text="Traffic Signal Status",
            title_font_color=COLOR_ACCENT,
            legend=dict(orientation="h"),
            margin=dict(l=10, r=10, t=50, b=10),
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)


# ======================================================================
# SECTION 8d - PEAK vs OFF-PEAK TRAFFIC (bar)
# ======================================================================
st.subheader("Peak vs Off-Peak Traffic")
df_peak = run_query(connection, QUERIES["peak_offpeak"])
if df_peak.empty:
    st.info("No time-dimension data available (needs the warehouse load).")
else:
    period_colors = {"Peak Hour": COLOR_RED, "Off-Peak": COLOR_GREEN}
    colors = [period_colors.get(p, "#9ca3af") for p in df_peak["period"]]
    # Bar height = number of records; label each bar with its avg volume.
    labels = [
        f"{int(r):,} records<br>avg vol {a:.0f}"
        for r, a in zip(df_peak["records"], df_peak["avg_volume"])
    ]
    fig = go.Figure(
        go.Bar(
            x=df_peak["period"],
            y=df_peak["records"],
            marker_color=colors,
            text=labels,
            textposition="outside",
        )
    )
    fig.update_yaxes(title_text="Number of records")
    st.plotly_chart(
        styled_layout(fig, "Peak vs Off-Peak (records + avg traffic volume)"),
        use_container_width=True,
    )


# Close the connection for this run.
try:
    connection.close()
except Exception:
    pass


# ======================================================================
# SECTION 9 - AUTO-REFRESH EVERY 5 SECONDS WITH COUNTDOWN
# ======================================================================
countdown_box = st.sidebar.empty()
for remaining in range(REFRESH_SECONDS, 0, -1):
    countdown_box.markdown(
        f"<p style='color:{COLOR_ACCENT};font-weight:600;'>"
        f"⟳ Refreshing in {remaining} second(s)...</p>",
        unsafe_allow_html=True,
    )
    time.sleep(1)

st.rerun()
