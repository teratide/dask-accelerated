from dask.distributed import Scheduler
from tornado.ioloop import IOLoop
from datetime import datetime
import asyncio
import pickle


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


def remove_non_accelerated_workers(scheduler):

    # New event loop to await async remove worker method
    loop = asyncio.new_event_loop()

    # TODO: fix this
    # Somehow this does not always work on the first try
    # A quick but messy fix is to run it more than once to
    # ensure all non-accelerated workers get removed
    for i in range(3):
        workers = scheduler.workers
        for worker in workers:
            # All accelerated workers are called 'accelerated-[timestamp]'
            if str(workers[worker].name).split('-')[0] != 'accelerated':
                loop.run_until_complete(
                    scheduler.remove_worker(address=worker)
                )

    loop.close()


def save_data(data):

    # Add timestamp to data
    timestamp = datetime.now().strftime("%d-%b-%Y_%H:%M:%S")
    data['timestamp'] = timestamp

    # Save data to disk
    data_root = '../notebooks/'
    with open(data_root + 'data-workers.pickle', 'wb') as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
