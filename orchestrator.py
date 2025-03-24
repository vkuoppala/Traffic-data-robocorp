from prefect import flow, task
from producer import produce_traffic_data
from consumer import consume_traffic_data

@task
def generate_work_items():
    return produce_traffic_data(save_to_file=False)  # Returns list of work items (JSON)

@task
def process_work_items(work_items):
    consume_traffic_data(work_items)  # In-memory passing

@flow(name="Traffic Data Orchestrator")
def traffic_pipeline():
    work_items = generate_work_items()
    process_work_items(work_items)

if __name__ == "__main__":
    traffic_pipeline()
