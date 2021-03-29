import dask
import time
from helpers import get_lazy_result
from optimization import optimize_graph

def run_re2():

    result = get_lazy_result(10)
    graph = result.__dask_graph__()

    dsk = optimize_graph(graph, 10)
    #  dsk = dict(graph)

    # Perform the computations in the graph
    start = time.time()
    res = dask.get(dsk, (result.__dask_layers__()[0], 0))
    end = time.time()

    print("\n\nRESULT: ", res, "\tcomputed in ", end-start, " seconds")

if __name__ == '__main__':
    run_re2()