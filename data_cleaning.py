"""
data_cleaning.py  (PHASE 1)
===========================
Reads the raw Kaggle dataset, cleans/enriches it, prints a full cleaning
report, and saves the result to traffic_cleaned.csv.

New engineered columns:
  - location_name   (mapped from location_id)
  - total_vehicles  (cars + trucks + bikes)
  - speed_category  (SLOW / NORMAL / FAST / DANGEROUS)
  - anomaly_type    (ACCIDENT_RISK / SPEEDING / CONGESTION / ACCIDENT / NORMAL)
  - severity        (HIGH / MEDIUM / LOW / NONE)

Run:  python data_cleaning.py
"""

import os

import pandas as pd

RAW_FILE = "smart_traffic_management_dataset.csv"
CLEAN_FILE = "traffic_cleaned.csv"

# location_id -> real Egyptian road name
LOCATION_MAP = {
    1: "Ring Road Cairo",
    2: "Corniche Alexandria",
    3: "Suez Road",
    4: "October Bridge",
    5: "Tahrir Square",
}


def speed_category(speed: float) -> str:
    """Bucket the average speed into a readable category."""
    if speed < 40:
        return "SLOW"
    if speed <= 80:
        return "NORMAL"
    if speed <= 120:
        return "FAST"
    return "DANGEROUS"


def classify_anomaly(row) -> str:
    """Apply the anomaly rules in priority order."""
    speed = row["avg_vehicle_speed"]
    volume = row["traffic_volume"]
    accident = row["accident_reported"]

    if accident == 1 and speed > 100:
        return "ACCIDENT_RISK"
    if speed > 120:
        return "SPEEDING"
    if volume > 700:
        return "CONGESTION"
    if accident == 1:
        return "ACCIDENT"
    return "NORMAL"


def classify_severity(row) -> str:
    """Assign a severity based on the anomaly type and its measurements."""
    a_type = row["anomaly_type"]
    speed = row["avg_vehicle_speed"]
    volume = row["traffic_volume"]

    if a_type == "ACCIDENT_RISK":
        return "HIGH"
    if a_type == "SPEEDING":
        return "HIGH" if speed > 150 else "MEDIUM"
    if a_type == "CONGESTION":
        if volume > 850:
            return "HIGH"
        if volume > 700:
            return "MEDIUM"
        return "LOW"
    if a_type == "ACCIDENT":
        return "HIGH"
    return "NONE"


def main() -> None:
    if not os.path.exists(RAW_FILE):
        print(f"ERROR: '{RAW_FILE}' not found in {os.getcwd()}")
        return

    print("Reading raw dataset...")
    df = pd.read_csv(RAW_FILE)
    original_shape = df.shape

    # 1) Proper datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 2) location_id -> location_name
    df["location_name"] = df["location_id"].map(LOCATION_MAP)

    # 3) total_vehicles
    df["total_vehicles"] = (
        df["vehicle_count_cars"]
        + df["vehicle_count_trucks"]
        + df["vehicle_count_bikes"]
    )

    # 4) speed_category
    df["speed_category"] = df["avg_vehicle_speed"].apply(speed_category)

    # 5) anomaly_type  (must be computed before severity)
    df["anomaly_type"] = df.apply(classify_anomaly, axis=1)

    # 6) severity
    df["severity"] = df.apply(classify_severity, axis=1)

    final_shape = df.shape

    # 7) Cleaning report
    print("\n" + "=" * 60)
    print(" CLEANING REPORT")
    print("=" * 60)
    print(f"Original shape : {original_shape[0]} rows x {original_shape[1]} cols")
    print(f"Final shape    : {final_shape[0]} rows x {final_shape[1]} cols")

    print("\n-- anomaly_type counts --")
    print(df["anomaly_type"].value_counts().to_string())

    print("\n-- severity counts --")
    print(df["severity"].value_counts().to_string())

    print("\n-- weather_condition counts --")
    print(df["weather_condition"].value_counts().to_string())

    print("\n-- speed_category counts --")
    print(df["speed_category"].value_counts().to_string())

    print("\n-- sample of 5 rows (new columns) --")
    sample_cols = [
        "timestamp", "location_name", "avg_vehicle_speed", "traffic_volume",
        "total_vehicles", "speed_category", "anomaly_type", "severity",
    ]
    print(df[sample_cols].head(5).to_string(index=False))

    # 8) Save
    df.to_csv(CLEAN_FILE, index=False)
    print("\n" + "=" * 60)
    print(f"Cleaned data saved to: {CLEAN_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
