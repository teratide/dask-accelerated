# TODO: include the testing and functionalities of this file into main.py

from dask.distributed import Client
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

    # Define a simple function and
    # submit a future on the client

    # Increment integer values by 1
    def inc(x):
        return x + 1

    # Y holds the future to the result
    y = client.submit(inc, 11)

    input("Press Enter to trigger computation...")
    print(y.result())

    input("Press Enter to close the client...")
    client.close()


if __name__ == '__main__':
    main()
