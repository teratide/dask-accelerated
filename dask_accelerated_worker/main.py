from dask.distributed import Client
from dask_accelerated import helpers
from dask_accelerated_worker import helpers as worker_helpers
import benchmarks
import time
from threading import Thread
import logging

logger = logging.getLogger(__name__)


benchmark_config = {
    'const_in_size': 4096e3,
    'in_sizes': [256e3, 512e3, 1024e3, 2048e3, 4096e3],
    'batch_size': 1e3,
    'const_batch_aggregate': 1e3,
    'batch_aggregates': [64, 128, 256, 512, 1024, 2048, 4096, 8192],
    'repeats': 5
}


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
    helpers.generate_datasets_if_needed(benchmark_config['in_sizes'], benchmark_config['batch_size'])

    benchmarks.warm_workers(client, scheduler, benchmark_config)

    data = {}

    # Keep running the benchmark until the user quits the client
    while True:
        data = benchmarks.run_all_benchmarks(client, scheduler, data, benchmark_config)

        user_input = input("Press Enter to run again or send 'q' to close the client...")
        if user_input == 'q':
            break

    worker_helpers.save_data(data)

    # Close the client
    client.close()


if __name__ == '__main__':
    main()
