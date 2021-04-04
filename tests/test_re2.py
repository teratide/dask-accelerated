import unittest
import dask_accelerated
from dask_accelerated import helpers


class TestRe2(unittest.TestCase):

    def test_small_input(self):
        """
        Test that the re2 implementation computes the same
        result as vanilla Dask for a small input dataset
        """
        in_size = 64e3
        batch_size = 1e6

        (res_vanilla, dur) = helpers.run_vanilla(in_size, batch_size)
        (res_re2, dur) = helpers.run_re2(in_size, batch_size)

        self.assertEqual(res_vanilla, res_re2)


if __name__ == '__main__':
    unittest.main()
