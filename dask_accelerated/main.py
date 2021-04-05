import benchmarks


if __name__ == '__main__':

    in_sizes = [1e3, 2e3, 4e3, 8e3, 16e3, 32e3, 64e3, 128e3, 256e3, 512e3, 1024e3, 2048e3]
    batch_aggregates = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]
    repeats = 1
    benchmarks.benchmark_tidre(in_sizes, batch_aggregates, repeats)
