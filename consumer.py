import requests
from typing import List, Dict

SALES_SYSTEM_URL = "https://robocorp.com/inhuman-insurance-inc/sales-system-api"

def is_valid_traffic_data(data: dict) -> bool:
    return isinstance(data.get("country"), str) and len(data["country"]) == 3

def post_traffic_data(traffic_data: dict) -> (int, dict):
    try:
        response = requests.post(SALES_SYSTEM_URL, json=traffic_data)
        return response.status_code, response.json()
    except requests.RequestException as e:
        return 0, {"message": str(e)}

def consume_traffic_data(work_items: List[Dict]):
    for item in work_items:
        traffic_data = item.get("payload", {}).get("traffic_data", {})
        
        if is_valid_traffic_data(traffic_data):
            status, response = post_traffic_data(traffic_data)

            if status != 200:
                # Retry once
                status, response = post_traffic_data(traffic_data)

                if status != 200:
                    print("APPLICATION ERROR,", "TRAFFIC_DATA_POST_FAILED,", response.get("message"), traffic_data)
        else:
            print("BUSINESS ERROR,", "INVALID_TRAFFIC_DATA,", item.get("payload", {}))
