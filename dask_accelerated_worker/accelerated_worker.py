import logging
from dask.distributed import Worker
from dask.distributed import worker

logger = logging.getLogger(__name__)


# Create an accelerated worker class based on the original worker class
class AcceleratedWorker(Worker):

    def add_task(
        self,
        key,
        function=None,
        args=None,
        kwargs=None,
        task=worker.no_value,
        who_has=None,
        nbytes=None,
        priority=None,
        duration=None,
        resource_restrictions=None,
        actor=False,
        **kwargs2,
    ):
        # TODO: substitute fpga accelerated op in task graph

        super().add_task(
            key,
            function,
            args,
            kwargs,
            task,
            who_has,
            nbytes,
            priority,
            duration,
            resource_restrictions,
            actor,
            **kwargs2,
        )
