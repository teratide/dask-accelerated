import helpers
import benchmarks

def test_re2():
    (vanilla_result, vanilla_duration) = helpers.run_vanilla(10)
    (dask_result, dask_duration) = helpers.run_re2(10)

    assert vanilla_result == dask_result

if __name__ == '__main__':

    sizes = [1e3, 64e3, 1024e3, 2048e3]
    repeats = 3

    helpers.generate_datasets_if_needed(sizes)

    benchmarks.benchmark_filter_duration(sizes, repeats)
