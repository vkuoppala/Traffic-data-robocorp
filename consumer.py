from prefect import get_run_logger
import requests
from typing import List, Dict

SALES_SYSTEM_URL = "https://robocorp.com/inhuman-insurance-inc/sales-system-api"

RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

def is_valid(data: dict) -> bool:
    return isinstance(data.get("country"), str) and len(data["country"]) == 3

def consume_traffic_data(work_items: List[Dict]):
    logger = get_run_logger()

    for item in work_items:
        traffic_data = item.get("payload", {}).get("traffic_data", {})
        if not is_valid(traffic_data):
            logger.error(
                f"{RED}BUSINESS ERROR: INVALID_TRAFFIC_DATA{RESET} - Payload: {item.get('payload')}"
            )
            continue

        for attempt in range(2):  # try once + retry
            response = requests.post(SALES_SYSTEM_URL, json=traffic_data)
            if response.status_code == 200:
                break
        else:
            logger.warning(
                f"{YELLOW}APPLICATION ERROR: TRAFFIC_DATA_POST_FAILED{RESET} - Message: {response.json().get('message')} - Data: {traffic_data}"
            )
