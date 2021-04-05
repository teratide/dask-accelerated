import pickler
from dask_accelerated import helpers


def run_repeats(in_size, batch_size, batch_aggregate, repeats, key, vanilla_filter, re2_filter, tidre_filter=None):

    # Single run to mitigate caching effects
    (res, dur) = helpers.run_vanilla(in_size, batch_size, batch_aggregate)
    (res, dur) = helpers.run_re2(in_size, batch_size, batch_aggregate)
    vanilla_filter[key] = 0
    re2_filter[key] = 0

    if tidre_filter is not None:
        (res, dur) = helpers.run_tidre(in_size, batch_size, batch_aggregate)
        tidre_filter[key] = 0

    for i in range(repeats):
        (res_vanilla, dur) = helpers.run_vanilla(in_size, batch_size, batch_aggregate)
        vanilla_filter[key] += dur['filter']['total']

        (res_re2, dur) = helpers.run_re2(in_size, batch_size, batch_aggregate)
        re2_filter[key] += dur['filter']['total']

        if tidre_filter is not None:
            (res_tidre, dur) = helpers.run_tidre(in_size, batch_size, batch_aggregate)
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
