import helpers
import pickler


def benchmark_filter_duration(sizes, repeats):

    vanilla_filter = {}
    re2_filter = {}
    for size in sizes:
        # Single run to mitigate caching effects
        (res, dur) = helpers.run_vanilla(size)
        (res, dur) = helpers.run_re2(size)

        # Append 'M' to size and use as key to indicate 10e6 rows
        key = helpers.make_size_string(size)
        vanilla_filter[key] = 0
        re2_filter[key] = 0

        for i in range(repeats):
            (res_vanilla, dur) = helpers.run_vanilla(size)
            vanilla_filter[key] += dur['filter']['total']

            (res_re2, dur) = helpers.run_re2(size)
            re2_filter[key] += dur['filter']['total']

            # Sanity check
            assert res_vanilla == res_re2

        # Average the duration
        vanilla_filter[key] = vanilla_filter[key] / repeats
        re2_filter[key] = re2_filter[key] / repeats

    print(vanilla_filter)
    print(re2_filter)

    data = {
        'vanilla_filter': vanilla_filter,
        're2_filter': re2_filter
    }

    pickler.store_to_notebooks(data)