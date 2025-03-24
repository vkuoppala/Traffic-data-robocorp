import requests
import json
import csv
import os
from collections import defaultdict
from typing import List, Dict

# Constants
TRAFFIC_JSON_URL = "https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json"
TRAFFIC_JSON_FILE_PATH = "./output/traffic.json"

# JSON data keys
COUNTRY_KEY = "SpatialDim"
YEAR_KEY = "TimeDim"
RATE_KEY = "NumericValue"
GENDER_KEY = "Dim1"

def produce_traffic_data(save_to_file: bool = False, output_path: str = "./output/output.json") -> List[Dict]:
    """
    Downloads, filters, and transforms traffic data into a list of work items.
    Returns a JSON-compatible object (list of work items).
    """
    json_data = download_and_load_traffic_data(TRAFFIC_JSON_URL)
    write_table_to_csv(json_data, TRAFFIC_JSON_FILE_PATH)

    filtered = filter_and_sort_traffic_data(json_data)
    latest_per_country = get_latest_data_by_country(filtered)
    payloads = create_work_item_payloads(latest_per_country)
    work_items = format_work_items(payloads)

    if save_to_file:
        save_work_items_to_file(work_items, output_path)

    return work_items

def download_and_load_traffic_data(url: str) -> List[Dict]:
    response = requests.get(url)
    response.raise_for_status()
    os.makedirs(os.path.dirname(TRAFFIC_JSON_FILE_PATH), exist_ok=True)
    with open(TRAFFIC_JSON_FILE_PATH, 'wb') as f:
        f.write(response.content)

    with open(TRAFFIC_JSON_FILE_PATH, 'r') as f:
        data = json.load(f)
    return data.get("value", [])

def write_table_to_csv(data: List[Dict], csv_path: str):
    if not data:
        return
    keys = data[0].keys()
    with open(csv_path.replace(".json", ".csv"), 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

def filter_and_sort_traffic_data(data: List[Dict]) -> List[Dict]:
    max_rate = 5.0
    both_genders = "BTSX"
    filtered = [
        row for row in data
        if row.get(RATE_KEY, float("inf")) < max_rate and row.get(GENDER_KEY) == both_genders
    ]
    return sorted(filtered, key=lambda x: x.get(YEAR_KEY, 0), reverse=True)

def get_latest_data_by_country(data: List[Dict]) -> List[Dict]:
    grouped = defaultdict(list)
    for row in data:
        grouped[row[COUNTRY_KEY]].append(row)
    return [sorted(rows, key=lambda x: x[YEAR_KEY], reverse=True)[0] for rows in grouped.values()]

def create_work_item_payloads(traffic_data: List[Dict]) -> List[Dict]:
    return [
        {
            "country": row[COUNTRY_KEY],
            "year": row[YEAR_KEY],
            "rate": row[RATE_KEY]
        }
        for row in traffic_data
    ]

def format_work_items(payloads: List[Dict]) -> List[Dict]:
    if not payloads:
        return [{"payload": {"message": "No payloads to save"}, "files": {}}]
    return [{"payload": {"traffic_data": payload}, "files": {}} for payload in payloads]

def save_work_items_to_file(work_items: List[Dict], file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as f:
        json.dump(work_items, f, indent=4)

# For testing or direct use
if __name__ == "__main__":
    work_items = produce_traffic_data(save_to_file=True)
    print(json.dumps(work_items, indent=2))
