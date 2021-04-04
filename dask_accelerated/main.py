import helpers
import benchmarks


def test_re2(size):
    (re2_result, re2_duration) = helpers.run_re2(size)
    (vanilla_result, vanilla_duration) = helpers.run_vanilla(size)

    assert vanilla_result == re2_result


def test_tidre(size):
    (tidre_result, tidre_duration) = helpers.run_tidre(size)
    (vanilla_result, vanilla_duration) = helpers.run_vanilla(size)

    assert vanilla_result == tidre_result


if __name__ == '__main__':

    in_sizes = [1e3, 2e3, 4e3, 8e3, 16e3, 32e3, 64e3, 128e3, 256e3, 512e3, 1024e3, 2048e3]
    batch_sizes = [1e3, 2e3, 4e3, 8e3, 16e3, 32e3, 64e3, 128e3, 256e3, 512e3, 1024e3, 2048e3]
    repeats = 3
    helpers.generate_datasets_if_needed(in_sizes)
    benchmarks.benchmark_re2(in_sizes, batch_sizes, repeats)
