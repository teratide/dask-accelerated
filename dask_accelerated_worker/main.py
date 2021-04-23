# TODO: include the testing and functionalities of this file into main.py

from dask.distributed import Client
from dask_accelerated import helpers
import asyncio
import logging
logger = logging.getLogger(__name__)


def main():
    # Start a client in local cluster mode and expose the underlying scheduler
    client = Client()
    scheduler = client.cluster.scheduler
    print('Dashboard available at', client.dashboard_link)

    loop = asyncio.get_event_loop()

    input("Press Enter to remove non accelerated workers...")

    # Remove the non-accelerated workers
    for i in range(3):
        workers = scheduler.workers
        for worker in workers:
            if workers[worker].name != 'accelerated':
                loop.run_until_complete(
                    scheduler.remove_worker(address=worker)
                )

    loop.close()

    input("Press Enter to submit task graph...")

    in_size = 64e3
    batch_size = 16e3

    # The actual batch size in computed by aggregating
    # multiple chunks of 1e3 records into the desired batch size
    batch_aggregate = int(batch_size / 1e3)
    batch_size = 1e3

    # Make sure the desired dataset exists
    helpers.generate_datasets_if_needed([in_size], batch_size)

    lazy_result = helpers.get_lazy_result(in_size, batch_size, batch_aggregate)
    graph = lazy_result.__dask_graph__()

    res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))

    print(res)

    input("Press Enter to close the client...")
    client.close()


if __name__ == '__main__':
    main()
