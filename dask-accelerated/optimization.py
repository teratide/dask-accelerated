import re
from dask.optimization import SubgraphCallable
from operators import custom_re2

# Unwrap the corresponding subgraph_callable in the task graph in order to insert a custom function
def compute_substitute(dsk, key, custom):
    str_match = dsk[(key, 0)]
    call = str_match[0]

    original_dsk = call.dsk

    vals = original_dsk[key]
    vals_args = vals[3]
    new_vals_args = (vals_args[0], [['_func', custom], vals_args[1][1]])
    new_vals = (vals[0], vals[1], vals[2], new_vals_args)
    new_dsk = {key: new_vals}

    return SubgraphCallable(new_dsk, call.outkey, call.inkeys, "regex_callable")

# Substitute all string match operators in the graph with the custom re2 operator
def optimize_graph_re2(graph, in_size):

    # All keys in the graph are concaternated with a hash of the operator
    # A simple way to find the correct key is to use a regular expression
    regex = re.compile('str-match.*')

    # Find the first key in the graph which matches the regex
    for key in graph.keys():
        if re.match(regex, key[0]) != None:
            key = key[0]    # The keys are tuples and the operator name is the first value
            break

    # Convert the HighLevelGraph to a normal task graph
    dsk = dict(graph)

    # Compute the substitute subgraph_callable
    regex_callable = compute_substitute(dsk, key, custom_re2)

    # TODO: Improve this, the number of worker splits are only equal to in_size if chunksize=1e6
    for i in range(in_size):
        target_layer = (key, i)
        target_op = list(dsk[target_layer])
        target_op[0] = regex_callable
        dsk[target_layer] = tuple(target_op)

    return dsk