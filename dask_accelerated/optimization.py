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
    # This key is used to target one of the operators in the task graph
    # from which the regex_callable will be constructed
    for key in graph.keys():
        if re.match(regex, key[0]) != None:
            key = key[0]    # The keys are tuples and the operator name is the first value
            break

    # Convert the HighLevelGraph to a normal task graph
    dsk = dict(graph)

    # Compute the substitute subgraph_callable
    regex_callable = compute_substitute(dsk, key, custom_re2)

    # Substitute the regex_callable if the operator name matches the str-match pattern
    for k in dsk:
        if re.match(regex, key[0]) != None:
            target_op = list(dsk[k])
            target_op[0] = regex_callable
            dsk[k] = tuple(target_op)

    return dsk