import dask
import time
from helpers import get_lazy_result
from optimization import optimize_graph

def run_vanilla(size):

    print("Running vanilla Dask on ", size, "M")

    result = get_lazy_result(size)
    graph = result.__dask_graph__()

    dsk = dict(graph)

    # Perform the computations in the graph
    start = time.time()
    res = dask.get(dsk, (result.__dask_layers__()[0], 0))
    end = time.time()

    duration_in_seconds = end - start

    return (res, duration_in_seconds)


def run_re2(size):
    print("Running RE2 on ", size, "M")

    result = get_lazy_result(size)
    graph = result.__dask_graph__()

    dsk = optimize_graph(graph, size)

    # Perform the computations in the graph
    start = time.time()
    res = dask.get(dsk, (result.__dask_layers__()[0], 0))
    end = time.time()

    duration_in_seconds = end - start

    return (res, duration_in_seconds)

if __name__ == '__main__':
    (res, duration_in_seconds) = run_re2(10)

    print("Computed ", res, " in ", duration_in_seconds, " seconds")