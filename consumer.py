import json
import requests
import os
import sys
from typing import Union, List, Dict

SALES_SYSTEM_URL = "https://robocorp.com/inhuman-insurance-inc/sales-system-api"

def is_valid_traffic_data(data: dict) -> bool:
    return isinstance(data.get("country"), str) and len(data["country"]) == 3

def post_traffic_data_to_sales_system(traffic_data: dict):
    try:
        response = requests.post(SALES_SYSTEM_URL, json=traffic_data)
        return response.status_code, response.json()
    except requests.RequestException as e:
        return 0, {"message": f"Request failed: {e}"}

def consume_traffic_data(work_items: Union[str, List[Dict]]):
    """
    Consumes traffic data from either a file path or directly from a list of work items (parsed JSON).
    """
    if isinstance(work_items, str):
        try:
            with open(work_items, 'r') as file:
                work_items = json.load(file)
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error reading input file: {e}", file=sys.stderr)
            sys.exit(1)

    for item in work_items:
        traffic_data = item.get("payload", {}).get("traffic_data", {})

        if is_valid_traffic_data(traffic_data):
            status, response = post_traffic_data_to_sales_system(traffic_data)

            if status != 200:
                # Retry once
                status, response = post_traffic_data_to_sales_system(traffic_data)

                if status != 200:
                    print(
                        "APPLICATION ERROR,",
                        "TRAFFIC_DATA_POST_FAILED,",
                        response.get("message", "Unknown error"),
                        traffic_data
                    )
        else:
            print(
                "BUSINESS ERROR,",
                "INVALID_TRAFFIC_DATA,",
                item.get("payload", {})
            )

def write_json_to_file(file_path: str, json_string: str):
    try:
        parsed_data = json.loads(json_string)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    try:
        with open(file_path, 'w') as f:
            json.dump(parsed_data, f, indent=4)
    except IOError as e:
        print(f"Error writing to file: {e}", file=sys.stderr)
        sys.exit(1)

def print_usage():
    print("Usage:")
    print("  python traffic_tool.py write <filePath> <jsonString>")
    print("  python traffic_tool.py consume [inputPath]")
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()

    command = sys.argv[1].lower()

    if command == "write":
        if len(sys.argv) != 4:
            print_usage()
        file_path = sys.argv[2]
        json_string = sys.argv[3]
        write_json_to_file(file_path, json_string)

    elif command == "consume":
        input_path = sys.argv[2] if len(sys.argv) > 2 else "output/workitems.json"
        consume_traffic_data(input_path)

    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        print_usage()
