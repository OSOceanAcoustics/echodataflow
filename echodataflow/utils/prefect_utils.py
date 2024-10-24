import logging
from prefect import get_client, task
from prefect.exceptions import ObjectNotFound
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.client.schemas.objects import WorkPool, WorkQueue
from prefect.client.orchestration import PrefectClient

from echodataflow.models.deployment.deployment import EDFWorkPool

# Get a logger instance to log messages throughout the code
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Function to get or create a work pool
async def _get_or_create_work_pool(client: PrefectClient, work_pool: WorkPool) -> WorkPool:
    """
    Retrieve an existing work pool or create a new one if it doesn't exist.

    Args:
        client (PrefectClient): The Prefect client instance to interact with the API.
        work_pool_name (str): The name of the work pool to retrieve or create.

    Returns:
        WorkPool: The existing or newly created work pool.
    """
    try:
        # Attempt to read the existing work pool by name
        work_pool = await client.read_work_pool(work_pool_name=work_pool.name)
        logger.info(f"Work Pool '{work_pool.name}' already exists.")
    except ObjectNotFound:
        # If the work pool does not exist, create a new one
        try:
            work_pool = await client.create_work_pool(
                WorkPoolCreate(
                    name=work_pool.name,
                    type=work_pool.type,
                    concurrency_limit=work_pool.concurrency_limit,
                )
            )
            logger.info(f"Created Work Pool '{work_pool.name}'.")
        except Exception as e:
            # Log an error message if work pool creation fails and re-raise the exception
            logger.error(f"Failed to create work pool '{work_pool.name}': {e}")
            raise e

    return work_pool


# Function to get or create a work queue
async def _get_or_create_work_queue(
    client: PrefectClient, work_queue_name: str, work_pool_name: str
) -> WorkQueue:
    """
    Retrieve an existing work queue or create a new one if it doesn't exist.

    Args:
        client (PrefectClient): The Prefect client instance to interact with the API.
        work_queue_name (str): The name of the work queue to retrieve or create.
        work_pool_name (str): The name of the work pool to associate with the work queue.

    Returns:
        WorkQueue: The existing or newly created work queue.
    """
    try:
        # Attempt to read the existing work queue by name within the specified work pool
        work_queue = await client.read_work_queue_by_name(
            name=work_queue_name, work_pool_name=work_pool_name
        )
        logger.info(f"Work Queue '{work_queue_name}' already exists.")
    except ObjectNotFound:
        # If the work queue does not exist, create a new one
        try:
            work_queue = await client.create_work_queue(
                name=work_queue_name, work_pool_name=work_pool_name
            )
            logger.info(f"Created Work Queue '{work_queue_name}' in Work Pool '{work_pool_name}'.")
        except Exception as e:
            # Log an error message if work queue creation fails and re-raise the exception
            logger.error(
                f"Failed to create work queue '{work_queue_name}' in work pool '{work_pool_name}': {e}"
            )
            raise e
    return work_queue


# Main function to create both work pool and work queue
@task
async def create_work_pool_and_queue(edf_work_pool: EDFWorkPool):
    """
    Ensure that the specified work pool and work queue are created.

    Args:
        work_pool_name (str): The name of the work pool to create.
        work_queue_name (str): The name of the work queue to create within the work pool.
    """
    # Use the Prefect client asynchronously to interact with the API
    async with get_client() as client:
        # Get or create the work pool
        work_pool = await _get_or_create_work_pool(client=client, work_pool=edf_work_pool)

        # If the work pool exists, proceed to get or create the work queue
        if work_pool:
            _ = await _get_or_create_work_queue(
                client=client,
                work_queue_name=edf_work_pool.workqueue_name,
                work_pool_name=edf_work_pool.name,
            )
