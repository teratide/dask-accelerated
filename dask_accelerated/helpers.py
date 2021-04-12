import dask
import time
import os
import numpy as np
from dask_accelerated.optimization import optimize_graph_re2
from dask_accelerated.operators import CustomFilter
from data_generator import gen


# Run the regex query on vanilla Dask
def run_vanilla(in_size, batch_size=1e6, split_row_groups=True):

    (result, graph) = get_result_and_graph(in_size, batch_size, split_row_groups, 'vanilla Dask')

    # Use the custom 'vanilla' str match operator which also records filter time
    substitute_operator = CustomFilter()

    # Optimize the task graph by substituting the custom filter operator
    dsk = optimize_graph_re2(graph, substitute_operator.custom_vanilla)

    (res, durations) = run_and_record_durations(dsk, result, substitute_operator)

    return res, durations


# Run the regex query on Dask + RE2
def run_re2(in_size, batch_size=1e6, split_row_groups=True):

    (result, graph) = get_result_and_graph(in_size, batch_size, split_row_groups, 'Dask + RE2')

    # Use the custom str match operator which also records filter time
    substitute_operator = CustomFilter()

    # Optimize the task graph by substituting the RE2 regex operator
    dsk = optimize_graph_re2(graph, substitute_operator.custom_re2)

    (res, durations) = run_and_record_durations(dsk, result, substitute_operator)

    return res, durations


# Run the regex query on Dask + RE2 (unaligned memory buffers)
def run_re2_unaligned(in_size, batch_size=1e6, split_row_groups=True):

    (result, graph) = get_result_and_graph(in_size, batch_size, split_row_groups, 'Dask + RE2 (unaligned)')

    # Use the custom str match operator which also records filter time
    substitute_operator = CustomFilter()

    # Optimize the task graph by substituting the RE2 regex operator
    dsk = optimize_graph_re2(graph, substitute_operator.custom_re2_unaligned)

    (res, durations) = run_and_record_durations(dsk, result, substitute_operator)

    return res, durations


# Run the regex query on Dask + Tidre
def run_tidre(in_size, batch_size=1e6, split_row_groups=True):

    (result, graph) = get_result_and_graph(in_size, batch_size, split_row_groups, 'Dask + Tidre')

    # Use the custom str match operator which also records filter time
    substitute_operator = CustomFilter()

    # Optimize the task graph by substituting the RE2 regex operator
    dsk = optimize_graph_re2(graph, substitute_operator.custom_tidre)

    (res, durations) = run_and_record_durations(dsk, result, substitute_operator)

    return res, durations


# Run the regex query on Dask + Tidre (unaligned memory buffers)
def run_tidre_unaligned(in_size, batch_size=1e6, split_row_groups=True):

    (result, graph) = get_result_and_graph(in_size, batch_size, split_row_groups, 'Dask + Tidre (unaligned)')

    # Use the custom str match operator which also records filter time
    substitute_operator = CustomFilter()

    # Optimize the task graph by substituting the RE2 regex operator
    dsk = optimize_graph_re2(graph, substitute_operator.custom_tidre_unaligned)

    (res, durations) = run_and_record_durations(dsk, result, substitute_operator)

    return res, durations


def get_result_and_graph(in_size, batch_size, split_row_groups, name):
    print("Running ", name, " \tin: ", in_size, " batch: ", batch_size*split_row_groups, "...\t", end="")

    # Obtain the HighLevelGraph for the usecase
    result = get_lazy_result(in_size, batch_size, split_row_groups)
    graph = result.__dask_graph__()

    return result, graph


# Returns a lazy result for the regex usecase in Dask
def get_lazy_result(in_size, batch_size, split_row_groups):

    # In size is in millions of records
    # Append 'M' to match the dataset filenames
    size_str = make_size_string(in_size)
    batch_str = make_size_string(batch_size)

    # Constants
    chunksize = batch_size
    parquet_engine = "pyarrow"  # Valid engines: ['fastparquet', 'pyarrow', 'pyarrow-dataset', 'pyarrow-legacy']
    file_root = "../data_generator/diving/data-"
    file_ext = ".parquet"
    regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'

    # Load the dataframe
    columns = ["value", "string"]
    filename = file_root + size_str + '-' + batch_str + file_ext
    df = dask.dataframe.read_parquet(
        filename,
        chunksize=chunksize,
        split_row_groups=split_row_groups,
        engine=parquet_engine,
        gather_statistics=True,
        columns=columns
    )

    # Define the query using the pandas-like API
    return df[df["string"].str.match(regex)]["value"].sum()


# Convert to size string to match output of data generator
def make_size_string(size):
    size = int(size)
    if (size >= 1e6):
        size_str = str(int(size / 1e6)) + 'M'
    else:
        size_str = str(size)

    return size_str


def run_and_record_durations(dsk, result, substitute_operator):

    # Perform the computations in the graph
    start = time.time()
    res = dask.get(dsk, (result.__dask_layers__()[0], 0))
    end = time.time()

    total_duration_in_seconds = end - start

    filter_durations = np.array(substitute_operator.durations)
    durations = construct_durations(total_duration_in_seconds, filter_durations)

    print("Computed ", res, " in ", total_duration_in_seconds, " seconds\tfilter: ", durations['filter']['total'], " seconds")

    return res, durations


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


def generate_datasets_if_needed(sizes, chunksize=1e6):
    data_root = '../data_generator/diving'

    missing_datasets = []
    for size in sizes:

        size_str = make_size_string(size)
        chunk_str = make_size_string(chunksize)

        file = '/data-' + size_str + '-' + chunk_str + '.parquet'

        # Check if the dataset exists
        if not os.path.isfile(data_root + file):
            missing_datasets.append(size)

    # Generate any missing datasets
    if len(missing_datasets) > 0:
        print("Missing datasets found, these will be generated")
        match_percentage = 0.05
        data_length = 100
        regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'
        parquet_chunksize = chunksize
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
