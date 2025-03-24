import requests
import json
from collections import defaultdict
from typing import List, Dict

# Constants for field keys
COUNTRY_KEY = "SpatialDim"
YEAR_KEY = "TimeDim"
RATE_KEY = "NumericValue"
GENDER_KEY = "Dim1"

TRAFFIC_JSON_URL = "https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json"

def fetch_raw_traffic_data() -> List[Dict]:
    """Fetches raw traffic data JSON from the remote source."""
    response = requests.get(TRAFFIC_JSON_URL)
    response.raise_for_status()
    json_data = response.json()
    return json_data.get("value", [])

def filter_and_sort_traffic_data(data: List[Dict]) -> List[Dict]:
    """Filters by max rate and gender, then sorts by year (desc)."""
    max_rate = 5.0
    filtered = [
        row for row in data
        if row.get(RATE_KEY, float("inf")) < max_rate and row.get(GENDER_KEY) == "BTSX"
    ]
    return sorted(filtered, key=lambda x: x.get(YEAR_KEY, 0), reverse=True)

def get_latest_data_by_country(data: List[Dict]) -> List[Dict]:
    """Keeps only the most recent record per country."""
    grouped = defaultdict(list)
    for row in data:
        grouped[row[COUNTRY_KEY]].append(row)
    return [sorted(rows, key=lambda x: x[YEAR_KEY], reverse=True)[0] for rows in grouped.values()]

def create_work_items(data: List[Dict]) -> List[Dict]:
    """Formats traffic records into work item format."""
    return [
        {"payload": {"traffic_data": {
            "country": row[COUNTRY_KEY],
            "year": row[YEAR_KEY],
            "rate": row[RATE_KEY]
        }}, "files": {}}
        for row in data
    ]

def produce_traffic_data() -> List[Dict]:
    """Main entrypoint: full producer pipeline returning in-memory work items."""
    raw_data = fetch_raw_traffic_data()
    filtered = filter_and_sort_traffic_data(raw_data)
    latest = get_latest_data_by_country(filtered)
    return create_work_items(latest)
