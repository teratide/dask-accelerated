import re
from dask.optimization import SubgraphCallable
from operators import custom_regex

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