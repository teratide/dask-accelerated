import dask
import dask.dataframe
import time
import pyarrow
import pandas
import re
from dask.dataframe.accessor import maybe_wrap_pandas
from dask.optimization import SubgraphCallable

# import dask_accelerated

def get_lazy_result(in_size):
    size = str(in_size) + "M"

    # Constants
    chunksize = 1e6
    parquet_engine = "pyarrow"
    file_root = "/home/bob/Documents/thesis/data/generator/diving/data-"
    file_ext = ".parquet"
    regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'

    # Register table
    filters = []
    columns = ["value", "string"]
    df = dask.dataframe.read_parquet(file_root + size + file_ext, chunksize=chunksize, engine=parquet_engine, gather_statistics=True, columns=columns)

    # Plan/optimize the query
    return df[df["string"].str.match(regex)]["value"].sum()

def custom_regex(obj, accessor, attr, args, kwargs):
    # number_of_records = obj.size
    # regex = args[0]
    #
    # arr = pyarrow.Array.from_pandas(obj)
    # in_buffers = arr.buffers()

    # Original computation
    out = getattr(getattr(obj, accessor, obj), attr)(*args, **kwargs)

    # out = pandas.array([False] * number_of_records * 8, dtype=bool, copy=False)
    # out_buffers = pyarrow.Array.from_pandas(original_out).buffers()
    #
    # dask_accelerated.re2Eval(
    #     number_of_records,
    #     regex,
    #     in_buffers[1].address,
    #     in_buffers[2].address,
    #     in_buffers[1].size,
    #     in_buffers[2].size,
    #     out_buffers[1].address,
    #     out_buffers[1].size
    # )
    #
    # out = pyarrow.Array.from_buffers(pyarrow.bool_(), number_of_records, out_buffers).to_pandas()
    #
    # orig = maybe_wrap_pandas(obj, original_out)
    # new = maybe_wrap_pandas(obj, out)

    return maybe_wrap_pandas(obj, out)


def optimize_graph(graph, in_size):
    regex = re.compile('str-match.*')
    for key in graph.keys():
        if re.match(regex, key[0]) != None:
            key = key[0]
            break

    dsk = dict(graph)
    str_match = dsk[(key, 0)]
    call = str_match[0]

    original_dsk = call.dsk

    vals = original_dsk[key]
    vals_args = vals[3]
    new_vals_args = (vals_args[0], [['_func', custom_regex], vals_args[1][1]])
    new_vals = (vals[0], vals[1], vals[2], new_vals_args)
    new_dsk = {key: new_vals}

    regex_callable = SubgraphCallable(new_dsk, call.outkey, call.inkeys, "regex_callable")

    for i in range(in_size):
        target_layer = (key, i)
        target_op = list(dsk[target_layer])
        target_op[0] = regex_callable
        dsk[target_layer] = tuple(target_op)

    return dsk

def test_regex():

    result = get_lazy_result(10)
    graph = result.__dask_graph__()

    dsk = optimize_graph(graph, 10)
    #  dsk = dict(graph)

    # Perform the computations in the graph
    start = time.time()
    res = dask.get(dsk, (result.__dask_layers__()[0], 0))
    end = time.time()

    print("\n\nRESULT: ", res, "\tcomputed in ", end-start, " seconds")

    assert True

if __name__ == '__main__':
    test_regex()