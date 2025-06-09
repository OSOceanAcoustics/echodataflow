from pathlib import Path
from prefect import deploy
from prefect import flow, task
import random

@task
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(100), k=10)]

@task
def process_customer(customer_id: str) -> str:
    # Process a single customer
    return f"Processed {customer_id}"

@flow
def flow_test() -> list[str]:
    customer_ids = get_customer_ids()
    # Map the process_customer task across all customer IDs
    results = process_customer.map(customer_ids)
    return results


if __name__ == "__main__":

    # flow_test.deploy(
    #     name="flow_test",
    #     # cron="*/3 * * * *",
    #     work_pool_name="local",
    # )
    # flow_test.from_source(
    #     source=str(Path(__file__).parent),
    #     entrypoint="flow_cloud_test.py:flow_test",
    # ).serve('flow_test_serve')

    flow_test.from_source(
        source=str(Path(__file__).parent),
        entrypoint="flow_cloud_test.py:flow_test",
    ).to_deployment(
        name="flow_test_deploy",
        work_pool_name="local",
    )