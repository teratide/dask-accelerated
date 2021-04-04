import unittest
from dask_accelerated import helpers


class TestTidre(unittest.TestCase):

    def run_and_assert_equal(self, in_size, batch_size):

        helpers.generate_datasets_if_needed([in_size])

        (res_vanilla, dur) = helpers.run_vanilla(in_size, batch_size)
        (res_re2, dur) = helpers.run_tidre(in_size, batch_size)

        self.assertEqual(res_vanilla, res_re2)

    def test_small_input(self):
        """
        Test that the tidre implementation computes the same
        result as vanilla Dask for a small input dataset.
        This dataset easily fits into a single recordbatch
        """
        self.run_and_assert_equal(64e3, 1e6)


    def test_large_input(self):
        """
       Test that the tidre implementation computes the same
       result as vanilla Dask for a large input dataset.
       This dataset fits into multiple recordbatches
       """
        self.run_and_assert_equal(1e6, 64e3)

if __name__ == '__main__':
    unittest.main()
