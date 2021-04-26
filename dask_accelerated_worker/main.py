# TODO: include the testing and functionalities of this file into main.py

from dask.distributed import Client
from dask_accelerated import helpers
import pickle
from datetime import datetime
import time
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
    accelerated_names = [
        'accelerated-0',
        'accelerated-1',
        'accelerated-2',
        'accelerated-3'
    ]
    for i in range(3):
        workers = scheduler.workers
        for worker in workers:
            if workers[worker].name not in accelerated_names:
                loop.run_until_complete(
                    scheduler.remove_worker(address=worker)
                )

    loop.close()

    input("Press Enter to perform benchmarks...")

    in_sizes = [1e3, 2e3, 4e3, 8e3, 16e3, 32e3, 64e3, 128e3, 256e3, 512e3, 1024e3, 2048e3, 4096e3]
    batch_size = 1e3
    batch_aggregate = 16
    repeats = 3

    data = {}

    for in_size in in_sizes:

        # Make sure the desired dataset exists
        helpers.generate_datasets_if_needed([in_size], batch_size)

        lazy_result = helpers.get_lazy_result(in_size, batch_size, batch_aggregate)
        graph = lazy_result.__dask_graph__()

        data[in_size] = 0

        for i in range(repeats):

            start = time.time()
            res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))
            end = time.time()

            duration = end - start
            data[in_size] += duration
            print('Computed ', res, ' in ', duration, ' seconds')

        data[in_size] = data[in_size] / repeats

    workers = scheduler.workers

    timestamp = datetime.now().strftime("%d-%b-%Y_%H:%M:%S")
    # Add timestamp to data
    data['timestamp'] = timestamp
    data_root = '../notebooks/'
    with open(data_root + 'data-' + str(len(workers)) + '.pickle', 'wb') as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)

    input("Press Enter to close the client...")
    client.close()


if __name__ == '__main__':
    main()
