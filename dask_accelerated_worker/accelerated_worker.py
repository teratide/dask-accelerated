import logging
import re
import pickle
from dask.distributed import Worker
from dask.distributed import worker
from dask.optimization import SubgraphCallable
from dask_accelerated.operators import CustomFilter

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
        regex = re.compile('.*str-match.*')
        if re.match(regex, key) != None:
            # This task matches the operation we want to perform on fpga
            func = pickle.loads(function)

            substitute_op = CustomFilter().custom_re2

            original_dsk = func.dsk
            vals = original_dsk[func.outkey]
            vals_args = vals[3]
            new_vals_args = (vals_args[0], [['_func', substitute_op], vals_args[1][1]])
            new_vals = (vals[0], vals[1], vals[2], new_vals_args)
            new_dsk = {func.outkey: new_vals}

            new_func = SubgraphCallable(new_dsk, func.outkey, func.inkeys, "regex_callable")
            function = pickle.dumps(new_func)

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
