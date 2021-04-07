import unittest
from dask_accelerated import helpers


class TestRe2(unittest.TestCase):

    def run_and_assert_equal(self, in_size, batch_size):

        helpers.generate_datasets_if_needed([in_size], batch_size)

        (res_vanilla, dur) = helpers.run_vanilla(in_size, batch_size)
        (res_re2, dur) = helpers.run_re2(in_size, batch_size)

        self.assertEqual(res_vanilla, res_re2)

    def test_small_input(self):
        """
        Test that the re2 implementation computes the same
        result as vanilla Dask for a small input dataset.
        This dataset easily fits into a single recordbatch
        """
        self.run_and_assert_equal(16e3, 32e3)


    def test_large_input(self):
        """
       Test that the re2 implementation computes the same
       result as vanilla Dask for a large input dataset.
       This dataset fits into multiple recordbatches
       """
        self.run_and_assert_equal(32e3, 16e3)


class TestRe2Unaligned(unittest.TestCase):

    def run_and_assert_equal(self, in_size, batch_size):

        helpers.generate_datasets_if_needed([in_size], batch_size)

        (res_vanilla, dur) = helpers.run_vanilla(in_size, batch_size)
        (res_re2, dur) = helpers.run_re2_unaligned(in_size, batch_size)

        self.assertEqual(res_vanilla, res_re2)

    def test_small_input(self):
        """
        Test that the re2 implementation computes the same
        result as vanilla Dask for a small input dataset.
        This dataset easily fits into a single recordbatch
        """
        self.run_and_assert_equal(16e3, 32e3)


    def test_large_input(self):
        """
       Test that the re2 implementation computes the same
       result as vanilla Dask for a large input dataset.
       This dataset fits into multiple recordbatches
       """
        self.run_and_assert_equal(32e3, 16e3)

if __name__ == '__main__':
    unittest.main()
