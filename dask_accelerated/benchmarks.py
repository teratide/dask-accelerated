import helpers
import pickler


def benchmark_re2(in_sizes, batch_sizes, repeats):

    # Constants when varying a single parameter
    constant_in_size = 1e6
    constant_batch_size = 1e6

    data = {}

    (benchmark_data, benchmark_name) = benchmark_re2_in_size(in_sizes, constant_batch_size, repeats)
    data[benchmark_name] = benchmark_data

    (benchmark_data, benchmark_name) = benchmark_re2_batch_size(constant_in_size, batch_sizes, repeats)
    data[benchmark_name] = benchmark_data

    print_and_store_with_or_without_tidre(data, False)


def benchmark_tidre(in_sizes, batch_sizes, repeats):

    # Constants when varying a single parameter
    constant_in_size = 1e6
    constant_batch_size = 1e6

    data = {}

    (benchmark_data, benchmark_name) = benchmark_tidre_in_size(in_sizes, constant_batch_size, repeats)
    data[benchmark_name] = benchmark_data

    (benchmark_data, benchmark_name) = benchmark_tidre_batch_size(constant_in_size, batch_sizes, repeats)
    data[benchmark_name] = benchmark_data

    print_and_store_with_or_without_tidre(data, True)


def benchmark_re2_batch_size(in_size, batch_sizes, repeats):
    name = 'batch_size'

    vanilla_filter = {}
    re2_filter = {}

    for batch_size in batch_sizes:
        # Make key more readable if size >= 1e6
        key = helpers.make_size_string(batch_size)

        (vanilla_filter, re2_filter) = run_repeats(
            in_size,
            batch_size,
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


def benchmark_tidre_batch_size(in_size, batch_sizes, repeats):
    name = 'batch_size'

    vanilla_filter = {}
    re2_filter = {}
    tidre_filter = {}

    for batch_size in batch_sizes:
        # Make key more readable if size >= 1e6
        key = helpers.make_size_string(batch_size)

        (vanilla_filter, re2_filter, tidre_filter) = run_repeats(
            in_size,
            batch_size,
            repeats,
            key,
            vanilla_filter,
            re2_filter,
            tidre_filter
        )

    data = {
        'vanilla_filter': vanilla_filter,
        're2_filter': re2_filter,
        'tidre_filter': tidre_filter
    }

    return data, name


def benchmark_re2_in_size(in_sizes, batch_size, repeats):
    name = 'in_size'

    vanilla_filter = {}
    re2_filter = {}

    for in_size in in_sizes:
        # Make key more readable if size >= 1e6
        key = helpers.make_size_string(in_size)

        (vanilla_filter, re2_filter) = run_repeats(
            in_size,
            batch_size,
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


def benchmark_tidre_in_size(in_sizes, batch_size, repeats):
    name = 'in_size'

    vanilla_filter = {}
    re2_filter = {}
    tidre_filter = {}

    for in_size in in_sizes:
        # Make key more readable if size >= 1e6
        key = helpers.make_size_string(in_size)

        (vanilla_filter, re2_filter, tidre_filter) = run_repeats(
            in_size,
            batch_size,
            repeats,
            key,
            vanilla_filter,
            re2_filter,
            tidre_filter
        )

    data = {
        'vanilla_filter': vanilla_filter,
        're2_filter': re2_filter,
        'tidre_filter': tidre_filter
    }

    return data, name


def run_repeats(in_size, batch_size, repeats, key, vanilla_filter, re2_filter, tidre_filter=None):

    # Single run to mitigate caching effects
    (res, dur) = helpers.run_vanilla(in_size, batch_size)
    (res, dur) = helpers.run_re2(in_size, batch_size)
    vanilla_filter[key] = 0
    re2_filter[key] = 0

    if tidre_filter is not None:
        (res, dur) = helpers.run_tidre(in_size, batch_size)
        tidre_filter[key] = 0

    for i in range(repeats):
        (res_vanilla, dur) = helpers.run_vanilla(in_size, batch_size)
        vanilla_filter[key] += dur['filter']['total']

        (res_re2, dur) = helpers.run_re2(in_size, batch_size)
        re2_filter[key] += dur['filter']['total']

        if tidre_filter is not None:
            (res_tidre, dur) = helpers.run_tidre(in_size, batch_size)
            tidre_filter[key] += dur['filter']['total']

            # Sanity check
            assert res_vanilla == res_re2 == res_tidre
        else:
            # Sanity check
            assert res_vanilla == res_re2

    # Average the duration
    vanilla_filter[key] = vanilla_filter[key] / repeats
    re2_filter[key] = re2_filter[key] / repeats

    if tidre_filter is not None:
        tidre_filter[key] = tidre_filter[key] / repeats
        return vanilla_filter, re2_filter, tidre_filter
    else:
        return vanilla_filter, re2_filter


def print_and_store_with_or_without_tidre(data, tidre=False):
    print(data)
    data['tidre'] = tidre
    pickler.store_to_notebooks(data)
