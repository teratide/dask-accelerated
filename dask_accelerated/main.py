import helpers

def test_re2():
    (vanilla_result, vanilla_duration) = helpers.run_vanilla(10)
    (dask_result, dask_duration) = helpers.run_re2(10)

    assert vanilla_result == dask_result

if __name__ == '__main__':

    test_re2()