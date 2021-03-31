import dask
import time
import numpy as np
from optimization import optimize_graph_re2
from operators import CustomFilter


# Returns a lazy result for the regex usecase in Dask
def get_lazy_result(in_size):

    # In size is in millions of records
    # Append 'M' to match the dataset filenames
    size = str(in_size) + "M"

    # Constants
    chunksize = 1e6
    parquet_engine = "pyarrow"  # Valid engines: ['fastparquet', 'pyarrow', 'pyarrow-dataset', 'pyarrow-legacy']
    file_root = "../data-generator/diving/data-"
    file_ext = ".parquet"
    regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'

    # Load the dataframe
    columns = ["value", "string"]
    filename = file_root + size + file_ext
    df = dask.dataframe.read_parquet(
        filename,
        chunksize=chunksize,
        engine=parquet_engine,
        gather_statistics=True,
        columns=columns
    )

    # Define the query using the pandas-like API
    return df[df["string"].str.match(regex)]["value"].sum()


def construct_durations(total, filter):

    durations = {
        'total': total,
        'filter': {
            'min': np.min(filter),
            'ave': np.mean(filter),
            'max': np.max(filter),
            'total': np.sum(filter)
        }
    }

    return durations


# Run the regex query on vanilla Dask
def run_vanilla(size):

    print("Running vanilla Dask on ", size, "M...\t", end="")

    # Obtain the HighLevelGraph for the usecase
    result = get_lazy_result(size)
    graph = result.__dask_graph__()

    # Use the custom 'vanilla' str match operator which also records filter time
    substitute_operator = CustomFilter()

    # Optimize the task graph by substituting the custom filter operator
    dsk = optimize_graph_re2(graph, substitute_operator.custom_vanilla)

    # Perform the computations in the graph
    start = time.time()
    res = dask.get(dsk, (result.__dask_layers__()[0], 0))
    end = time.time()

    total_duration_in_seconds = end - start

    print("Computed ", res, " in ", total_duration_in_seconds, " seconds")

    filter_durations = np.array(substitute_operator.durations)
    durations = construct_durations(total_duration_in_seconds, filter_durations)

    return (res, durations)


# Run the regex query on Dask + RE2
def run_re2(size):
    print("Running Dask + RE2 on ", size, "M...\t\t", end="")

    # Obtain the HighLevelGraph for the usecase
    result = get_lazy_result(size)
    graph = result.__dask_graph__()

    # Use the custom str match operator which also records filter time
    substitute_operator = CustomFilter()

    # Optimize the task graph by substituting the RE2 regex operator
    dsk = optimize_graph_re2(graph, substitute_operator.custom_re2)

    # Perform the computations in the graph
    start = time.time()
    res = dask.get(dsk, (result.__dask_layers__()[0], 0))
    end = time.time()

    total_duration_in_seconds = end - start

    print("Computed ", res, " in ", total_duration_in_seconds, " seconds")

    filter_durations = np.array(substitute_operator.durations)
    durations = construct_durations(total_duration_in_seconds, filter_durations)

    return (res, durations)