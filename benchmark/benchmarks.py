import helpers as benchmark_helpers
from dask_accelerated import helpers


def benchmark_re2_batch_size(in_size, batch_size, batch_aggregates, repeats):
    name = 'batch_size'

    vanilla_filter = {}
    re2_filter = {}

    for batch_aggregate in batch_aggregates:
        # Make key more readable if size >= 1e6
        key = helpers.make_size_string(batch_aggregate*batch_size)

        (vanilla_filter, re2_filter) = benchmark_helpers.run_repeats(
            in_size,
            batch_size,
            batch_aggregate,
            repeats,
            key,
            vanilla_filter,
            re2_filter
        )

    data = {
        'vanilla_filter': vanilla_filter,
        're2_filter': re2_filter
    }

    return data, name


def benchmark_tidre_batch_size(in_size, batch_size, batch_aggregates, repeats):
    name = 'batch_size'

    vanilla_filter = {}
    re2_filter = {}
    tidre_filter = {}
    tidre_filter_unaligned = {}

    for batch_aggregate in batch_aggregates:
        # Make key more readable if size >= 1e6
        key = helpers.make_size_string(batch_aggregate*batch_size)

        (vanilla_filter, re2_filter, tidre_filter, tidre_filter_unaligned) = benchmark_helpers.run_repeats(
            in_size,
            batch_size,
            batch_aggregate,
            repeats,
            key,
            vanilla_filter,
            re2_filter,
            tidre_filter,
            tidre_filter_unaligned
        )

    data = {
        'vanilla_filter': vanilla_filter,
        're2_filter': re2_filter,
        'tidre_filter': tidre_filter,
        'tidre_filter_unaligned': tidre_filter_unaligned
    }

    return data, name


def benchmark_re2_in_size(in_sizes, batch_size, batch_aggregate, repeats):
    name = 'in_size'

    vanilla_filter = {}
    re2_filter = {}

    for in_size in in_sizes:
        # Make key more readable if size >= 1e6
        key = helpers.make_size_string(in_size)

        (vanilla_filter, re2_filter) = benchmark_helpers.run_repeats(
            in_size,
            batch_size,
            batch_aggregate,
            repeats,
            key,
            vanilla_filter,
            re2_filter
        )

    data = {
        'vanilla_filter': vanilla_filter,
        're2_filter': re2_filter
    }

    return data, name


def benchmark_tidre_in_size(in_sizes, batch_size, batch_aggregate, repeats):
    name = 'in_size'

    vanilla_filter = {}
    re2_filter = {}
    tidre_filter = {}
    tidre_filter_unaligned = {}

    for in_size in in_sizes:
        # Make key more readable if size >= 1e6
        key = helpers.make_size_string(in_size)

        (vanilla_filter, re2_filter, tidre_filter, tidre_filter_unaligned) = benchmark_helpers.run_repeats(
            in_size,
            batch_size,
            batch_aggregate,
            repeats,
            key,
            vanilla_filter,
            re2_filter,
            tidre_filter,
            tidre_filter_unaligned
        )

    data = {
        'vanilla_filter': vanilla_filter,
        're2_filter': re2_filter,
        'tidre_filter': tidre_filter,
        'tidre_filter_unaligned': tidre_filter_unaligned
    }

    return data, name



