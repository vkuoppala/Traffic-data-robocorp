from prefect import flow, task, get_run_logger
from producer import produce_traffic_data
from consumer import consume_traffic_data

@task(log_prints=True, retries=2, retry_delay_seconds=5)
def generate_work_items():
    logger = get_run_logger()
    logger.info("Fetching and preparing traffic data...")
    return produce_traffic_data()

@task(log_prints=True, retries=1)
def process_work_items(work_items):
    logger = get_run_logger()
    logger.info(f"Processing {len(work_items)} work items...")
    consume_traffic_data(work_items)

@flow(name="Traffic Data Orchestrator", log_prints=True)
def traffic_pipeline():
    items = generate_work_items()
    process_work_items(items)

if __name__ == "__main__":
    traffic_pipeline()
