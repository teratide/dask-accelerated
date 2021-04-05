import argparse
import benchmarks as bench
import helpers as benchmark_helpers
from dask_accelerated import helpers


# Parse command line arguments
parser = argparse.ArgumentParser(description='Benchmark the RE2 and Tidre implementations of dask_accelerated.')
parser.add_argument('--tidre', dest='tidre', action='store_const',
                    const=True, default=False,
                    help='run the Tidre benchmarks. (default: only run RE2)')

args = parser.parse_args()

def benchmark_re2(in_sizes, batch_aggregates, repeats):

    # Constants when varying a single parameter
    constant_in_size = 1e6
    constant_batch_size_in_benchmark = 1e6
    constant_batch_size_batch_benchmark = 1e3
    constant_batch_aggregate = 1

    helpers.generate_datasets_if_needed(in_sizes, constant_batch_size_in_benchmark)
    helpers.generate_datasets_if_needed(in_sizes, constant_batch_size_batch_benchmark)

    data = {}

    (benchmark_data, benchmark_name) = bench.benchmark_re2_in_size(in_sizes, constant_batch_size_in_benchmark, constant_batch_aggregate, repeats)
    data[benchmark_name] = benchmark_data

    (benchmark_data, benchmark_name) = bench.benchmark_re2_batch_size(constant_in_size, constant_batch_size_batch_benchmark, batch_aggregates, repeats)
    data[benchmark_name] = benchmark_data

    benchmark_helpers.print_and_store_with_or_without_tidre(data, False)


def benchmark_tidre(in_sizes, batch_aggregates, repeats):

    # Constants when varying a single parameter
    constant_in_size = 1e6
    constant_batch_size_in_benchmark = 1e6
    constant_batch_size_batch_benchmark = 1e3
    constant_batch_aggregate = 1

    helpers.generate_datasets_if_needed(in_sizes, constant_batch_size_in_benchmark)
    helpers.generate_datasets_if_needed(in_sizes, constant_batch_size_batch_benchmark)

    data = {}

    (benchmark_data, benchmark_name) = bench.benchmark_tidre_in_size(in_sizes, constant_batch_size_in_benchmark, constant_batch_aggregate, repeats)
    data[benchmark_name] = benchmark_data

    (benchmark_data, benchmark_name) = bench.benchmark_tidre_batch_size(constant_in_size, constant_batch_size_batch_benchmark, batch_aggregates, repeats)
    data[benchmark_name] = benchmark_data

    benchmark_helpers.print_and_store_with_or_without_tidre(data, True)


if __name__ == '__main__':

    in_sizes = [1e3, 2e3, 4e3, 8e3, 16e3, 32e3, 64e3, 128e3, 256e3, 512e3, 1024e3, 2048e3]
    batch_aggregates = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]
    repeats = 3

    benchmark_re2(in_sizes, batch_aggregates, repeats)
    if args.tidre:
        benchmark_tidre(in_sizes, batch_aggregates, repeats)

