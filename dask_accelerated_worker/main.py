from dask.distributed import Client
from dask_accelerated import helpers
from dask_accelerated_worker import helpers as worker_helpers
import pickle
from datetime import datetime
import time
from threading import Thread
import logging

logger = logging.getLogger(__name__)


# Benchmark configuration
const_in_size = 4096e3
in_sizes = [256e3, 512e3, 1024e3, 2048e3, 4096e3]
batch_size = 1e3
const_batch_aggregate = 1e3
batch_aggregates = [64, 128, 256, 512, 1024, 2048, 4096, 8192]
repeats = 5


def main():

    # Start a dask scheduler
    (scheduler, scheduler_loop) = worker_helpers.get_scheduler()
    thread = Thread(target=worker_helpers.run_scheduler, args=(scheduler, scheduler_loop,))
    thread.start()

    # Wait 1 second to ensure the scheduler is running
    time.sleep(1)

    # Start a client and connect it to the scheduler
    client = Client(scheduler.address)

    print('Dashboard available at', client.dashboard_link)
    print('Scheduler address: ', scheduler.address)

    # input("Press Enter to remove non accelerated workers...")
    # worker_helpers.remove_non_accelerated_workers(scheduler)

    input("Press Enter to perform benchmarks...")

    # Make sure the desired dataset exists
    helpers.generate_datasets_if_needed(in_sizes, batch_size)

    # Keep running the benchmark until the user quits the client
    while True:
        data = {}
        data_in_size = {}
        data_batch_size = {}

        # In size benchmark
        for in_size in in_sizes:

            lazy_result = helpers.get_lazy_result(in_size, batch_size, const_batch_aggregate)
            graph = lazy_result.__dask_graph__()

            # Dry run
            # res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))

            data_in_size[in_size] = 0

            for i in range(repeats):

                start = time.time()
                res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))
                end = time.time()

                duration = end - start
                data_in_size[in_size] += duration
                print(
                    'In: ', in_size,
                    '\tBatch: ', batch_size * const_batch_aggregate,
                    '\tComputed ', res, ' in ', duration, ' seconds'
                )

            data_in_size[in_size] = data_in_size[in_size] / repeats

        # Batch size benchmark
        for batch_aggregate in batch_aggregates:

            lazy_result = helpers.get_lazy_result(const_in_size, batch_size, batch_aggregate)
            graph = lazy_result.__dask_graph__()

            # Dry run
            # res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))

            data_batch_size[batch_aggregate] = 0

            for i in range(repeats):
                start = time.time()
                res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))
                end = time.time()

                duration = end - start
                data_batch_size[batch_aggregate] += duration
                print(
                    'In: ', const_in_size,
                    '\tBatch: ', batch_size * batch_aggregate,
                    'Computed ', res, ' in ', duration, ' seconds'
                )

            data_batch_size[batch_aggregate] = data_batch_size[batch_aggregate] / repeats

        # Count the number of accelerated workers
        accelerated_workers = 0
        for worker in scheduler.workers:
            if str(scheduler.workers[worker].name).split('-')[0] == 'accelerated':
                accelerated_workers += 1

        data[str(accelerated_workers)]['in_size'] = data_in_size
        data[str(accelerated_workers)]['batch_size'] = data_batch_size

        # Add timestamp to data
        timestamp = datetime.now().strftime("%d-%b-%Y_%H:%M:%S")
        data['timestamp'] = timestamp

        user_input = input("Press Enter to run again or send 'q' to close the client...")
        if user_input == 'q':
            break

    data_root = '../notebooks/'
    with open(data_root + 'data-workers.pickle', 'wb') as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)

    client.close()


if __name__ == '__main__':
    main()
