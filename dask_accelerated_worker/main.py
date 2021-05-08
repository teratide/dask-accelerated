# TODO: include the testing and functionalities of this file into main.py

from dask.distributed import Client, Scheduler
from dask_accelerated import helpers
from dask_accelerated_worker.utils import install_signal_handlers
import pickle
from datetime import datetime
import time
import asyncio
import logging
from tornado.ioloop import IOLoop
from threading import Thread
logger = logging.getLogger(__name__)


def get_scheduler():
    kwargs = {
        'preload': (),
        'preload_argv': (),
        'interface': None,
        'protocol': None,
        'scheduler_file': '',
        'idle_timeout': None
    }

    loop = IOLoop.current()
    sec = {}
    host = ''
    port = 8786
    dashboard = True
    dashboard_address = 8787
    dashboard_prefix = ''

    scheduler = Scheduler(
        loop=loop,
        security=sec,
        host=host,
        port=port,
        dashboard=dashboard,
        dashboard_address=dashboard_address,
        http_prefix=dashboard_prefix,
        **kwargs
    )

    return scheduler, loop


def run_scheduler(scheduler, loop):

    async def run():
        await scheduler
        await scheduler.finished()

    loop.run_sync(run)

    # install_signal_handlers(loop)
    #
    # async def run():
    #     await scheduler
    #     await scheduler.finished()
    #
    # try:
    #     loop.run_sync(run)
    # finally:
    #     scheduler.stop()
    #
    #     logger.info("End scheduler at %r", scheduler.address)


def main():
    # Start a client in local cluster mode and expose the underlying scheduler
    # client = Client(args.scheduler_address)
    # scheduler = client.cluster.scheduler
    # print('Dashboard available at', client.dashboard_link)
    # print('Scheduler address: ', scheduler.address)

    (scheduler, scheduler_loop) = get_scheduler()

    thread = Thread(target=run_scheduler, args=(scheduler, scheduler_loop, ))
    thread.start()

    time.sleep(5)

    # print('Dashboard available at', scheduler.dashboard_link)
    print('Scheduler address: ', scheduler.address)

    client = Client(scheduler.address)

    # loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()

    input("Press Enter to remove non accelerated workers...")

    # Remove the non-accelerated workers
    for i in range(3):
        workers = scheduler.workers
        for worker in workers:
            if str(workers[worker].name).split('-')[0] != 'accelerated':
                loop.run_until_complete(
                    scheduler.remove_worker(address=worker)
                )

    # loop.close()

    input("Press Enter to perform benchmarks...")

    const_in_size = 4096e3
    in_sizes = [512e3, 1024e3, 2048e3, 4096e3, 8192e3]
    batch_size = 1e3
    const_batch_aggregate = 1e3
    batch_aggregates = [64, 128, 256, 512, 1024, 2048, 4096, 8192]
    repeats = 5

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
            res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))

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
            res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))

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
            if str(workers[worker].name).split('-')[0] == 'accelerated':
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
