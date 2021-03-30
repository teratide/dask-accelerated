import dask
import time
from helpers import get_lazy_result
from optimization import optimize_graph_re2

# Run the regex query on vanilla Dask
def run_vanilla(size):

    print("Running vanilla Dask on ", size, "M")

    # Obtain the HighLevelGraph for the usecase
    result = get_lazy_result(size)
    graph = result.__dask_graph__()

    # Convert the HighLevelGraph to a simpler task graph matching the Dask accelerated implementation
    dsk = dict(graph)

    # Perform the computations in the graph
    start = time.time()
    res = dask.get(dsk, (result.__dask_layers__()[0], 0))
    end = time.time()

    duration_in_seconds = end - start

    return (res, duration_in_seconds)

# Run the regex query on Dask + RE2
def run_re2(size):
    print("Running RE2 on ", size, "M")

    # Obtain the HighLevelGraph for the usecase
    result = get_lazy_result(size)
    graph = result.__dask_graph__()

    # Optimize the task graph by substituting the RE2 regex operator
    dsk = optimize_graph_re2(graph, size)

    # Perform the computations in the graph
    start = time.time()
    res = dask.get(dsk, (result.__dask_layers__()[0], 0))
    end = time.time()

    duration_in_seconds = end - start

    return (res, duration_in_seconds)

if __name__ == '__main__':

    (result, duration_in_seconds) = run_vanilla(10)
    print("Computed ", result, " in ", duration_in_seconds, " seconds\n")

    (result, duration_in_seconds) = run_re2(10)
    print("Computed ", result, " in ", duration_in_seconds, " seconds\n")