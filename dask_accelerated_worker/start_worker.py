from dask_accelerated_worker.accelerated_worker import AcceleratedWorker
from dask.distributed import Worker
from tornado.ioloop import IOLoop
import asyncio
import sys
import signal
import logging
import argparse
import time
logger = logging.getLogger(__name__)


parser = argparse.ArgumentParser(description='Dask Accelerated Worker.')
parser.add_argument('scheduler_address', metavar='S', type=str,
                    help='string containing the ip and port of the scheduler. Example: tcp://127.0.0.1:37983')
parser.add_argument('--accelerated', dest='accelerated', action='store_const',
                    const=True, default=False,
                    help='use the accelerated worker implementation. (default: do not use)')

args = parser.parse_args()


def install_signal_handlers(loop=None, cleanup=None):
    """
    Install global signal handlers to halt the Tornado IOLoop in case of
    a SIGINT or SIGTERM.  *cleanup* is an optional callback called,
    before the loop stops, with a single signal number argument.
    """
    import signal

    loop = loop or IOLoop.current()

    old_handlers = {}

    def handle_signal(sig, frame):
        async def cleanup_and_stop():
            try:
                if cleanup is not None:
                    await cleanup(sig)
            finally:
                loop.stop()

        loop.add_callback_from_signal(cleanup_and_stop)
        # Restore old signal handler to allow for a quicker exit
        # if the user sends the signal again.
        signal.signal(sig, old_handlers[sig])

    for sig in [signal.SIGINT, signal.SIGTERM]:
        old_handlers[sig] = signal.signal(sig, handle_signal)


def main():

    scheduler_address = args.scheduler_address

    if args.accelerated:
        t = AcceleratedWorker
        worker_name = 'accelerated-' + str(time.time())
    else:
        t = Worker
        worker_name = 'vanilla-' + str(time.time())

    # Start a new worker based on the AcceleratedWorker class
    # This worker automatically connects to the scheduler and gets added to the worker pool
    kwargs = {
        'preload': (),
        'memory_limit': '0',
        'preload_argv': (),
        'interface': None,
        'protocol': None,
        'reconnect': True,
        'local_directory': None,
        'death_timeout': None,
        'lifetime': None,
        'lifetime_stagger': '0 seconds',
        'lifetime_restart': False
    }

    loop = IOLoop.current()

    async_loop = asyncio.get_event_loop()
    worker = async_loop.run_until_complete(
        t(
            scheduler_address,
            scheduler_file=None,
            nthreads=1,
            loop=loop,
            resources=None,
            security={},
            contact_address=None,
            host=None,
            port=None,
            dashboard=True,
            dashboard_address=':0',
            name=worker_name,
            **kwargs
        )
    )

    nannies = [worker]
    nanny = True

    async def close_all():
        # Unregister all workers from scheduler
        if nanny:
            await asyncio.gather(*[n.close(timeout=2) for n in nannies])

    signal_fired = False

    def on_signal(signum):
        nonlocal signal_fired
        signal_fired = True
        if signum != signal.SIGINT:
            logger.info("Exiting on signal %d", signum)
        return asyncio.ensure_future(close_all())

    async def run():
        await asyncio.gather(*nannies)
        await asyncio.gather(*[n.finished() for n in nannies])

    install_signal_handlers(loop, cleanup=on_signal)

    try:
        loop.run_sync(run)
    except TimeoutError:
        # We already log the exception in nanny / worker. Don't do it again.
        if not signal_fired:
            logger.info("Timed out starting worker")
        sys.exit(1)
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("End worker")


if __name__ == '__main__':
    main()
