import dask
import time
import os
import numpy as np
from optimization import optimize_graph_re2
from operators import CustomFilter
from data_generator import gen


# Convert to size string to match output of data generator
def make_size_string(size):
    size = int(size)
    if (size >= 1e6):
        size_str = str(int(size / 1e6)) + 'M'
    else:
        size_str = str(size)

    return size_str

# Returns a lazy result for the regex usecase in Dask
def get_lazy_result(in_size):

    # In size is in millions of records
    # Append 'M' to match the dataset filenames
    size_str = make_size_string(in_size)

    # Constants
    chunksize = 1e6
    parquet_engine = "pyarrow"  # Valid engines: ['fastparquet', 'pyarrow', 'pyarrow-dataset', 'pyarrow-legacy']
    file_root = "../data_generator/diving/data-"
    file_ext = ".parquet"
    regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'

    # Load the dataframe
    columns = ["value", "string"]
    filename = file_root + size_str + file_ext
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

    print("Running vanilla Dask on ", size, "...\t", end="")

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
    print("Running Dask + RE2 on ", size, "...\t\t", end="")

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

def generate_datasets_if_needed(sizes):
    data_root = '../data_generator/diving'

    missing_datasets = []
    for size in sizes:

        size_str = make_size_string(size)

        file = '/data-' + size_str + '.parquet'

        # Check if the dataset exists
        if not os.path.isfile(data_root + file):
            missing_datasets.append(size)

    # Generate any missing datasets
    if len(missing_datasets) > 0:
        print("Missing datasets found, these will be generated")
        match_percentage = 0.05
        data_length = 100
        regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'
        parquet_chunksize = 1e6
        parquet_compression = 'none'

        gen.regex_strings(
            missing_datasets,
            match_percentage,
            data_length,
            regex,
            parquet_chunksize,
            parquet_compression,
            data_root
        )
    else:
        print("No missing datasets found")
