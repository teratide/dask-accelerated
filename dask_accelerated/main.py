import argparse
import helpers


# Custom conversion to support strings of '64e3' to int
def scientific_to_int(val):
    return int(float(val))


# Parse command line arguments
parser = argparse.ArgumentParser(description='Dask Accelerated.')
parser.add_argument('in_size', metavar='N', type=scientific_to_int,
                    help='input size in records at which to run dask accelerated')
parser.add_argument('batch_size', metavar='B', type=scientific_to_int,
                    help='batch size in records at which to run dask accelerated, must be a multiple of 1000')
parser.add_argument('repeats', metavar='R', type=scientific_to_int,
                    help='the number of times to repeat the experiment')
parser.add_argument('--vanilla', dest='vanilla', action='store_const',
                    const=True, default=False,
                    help='run vanilla Dask. (default: do not run)')
parser.add_argument('--re2', dest='re2', action='store_const',
                    const=True, default=False,
                    help='run the RE2 implementation. (default: do not run)')
parser.add_argument('--tidre', dest='tidre', action='store_const',
                    const=True, default=False,
                    help='run the Tidre implementation. (default: do not run)')

args = parser.parse_args()


if __name__ == '__main__':

    in_size = args.in_size
    batch_size = args.batch_size
    repeats = args.repeats

    # The actual batch size in computed by aggregating
    # multiple chunks of 1e3 records into the desired batch size
    batch_aggregate = int(batch_size / 1e3)
    batch_size = 1e3

    # Make sure the desired dataset exists
    helpers.generate_datasets_if_needed([in_size], batch_size)

    if args.vanilla:
        filter_duration = 0
        for i in range(repeats):
            (res, dur) = helpers.run_vanilla(in_size, batch_size, batch_aggregate)
            filter_duration += dur['filter']['total']
        filter_duration = filter_duration / repeats
        print("Ran vanilla ", repeats, " times, ave total filter: ", filter_duration, " seconds")

    if args.re2:
        filter_duration = 0
        for i in range(repeats):
            (res, dur) = helpers.run_re2(in_size, batch_size, batch_aggregate)
            filter_duration += dur['filter']['total']
        filter_duration = filter_duration / repeats
        print("Ran RE2 ", repeats, " times, ave total filter: ", filter_duration, " seconds")

    if args.tidre:
        filter_duration = 0
        for i in range(repeats):
            (res, dur) = helpers.run_tidre(in_size, batch_size, batch_aggregate)
            filter_duration += dur['filter']['total']
        filter_duration = filter_duration / repeats
        print("Ran Tidre ", repeats, " times, ave total filter: ", filter_duration, " seconds")
