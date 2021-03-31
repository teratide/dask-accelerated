import helpers

def test_re2(size):
    (re2_result, re2_duration) = helpers.run_re2(size)
    (vanilla_result, vanilla_duration) = helpers.run_vanilla(size)

    assert vanilla_result == re2_result

def test_tidre(size):
    (tidre_result, tidre_duration) = helpers.run_tidre(size)
    (vanilla_result, vanilla_duration) = helpers.run_vanilla(size)

    assert vanilla_result == tidre_result

if __name__ == '__main__':

    size = 100;

    helpers.generate_datasets_if_needed([100])

    test_tidre(size)
