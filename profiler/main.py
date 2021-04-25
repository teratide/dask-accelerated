import cProfile
import pstats
import io
import argparse
from dask_accelerated import helpers

# Parse command line arguments
parser = argparse.ArgumentParser(description='Profile the RE2 and Tidre implementations of dask_accelerated.')
parser.add_argument('--tidre', dest='tidre', action='store_const',
                    const=True, default=False,
                    help='profile the Tidre implementation. (default: don\'t profile)')

args = parser.parse_args()


def profile_and_save(func, in_size, batch_size, out_file):

    # Create a new profiler and enable it,
    # recording all functions running from this point on
    pr = cProfile.Profile()
    pr.enable()

    # Run the function we want to profile
    func(in_size, batch_size)

    # Stop the profiler and convert the output to a stream
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
    ps.print_stats()

    # Write stream to file
    with open('../profiler/' + out_file, 'w+') as f:
        f.write(s.getvalue())


if __name__ == '__main__':

    in_size = 1e6
    batch_size = 1e6

    helpers.generate_datasets_if_needed([in_size], batch_size)

    profile_and_save(helpers.run_vanilla, in_size, batch_size, 'vanilla_prof.txt')
    profile_and_save(helpers.run_re2, in_size, batch_size, 're2_prof.txt')

    if args.tidre:
        profile_and_save(helpers.run_tidre, in_size, batch_size, 'tidre_prof.txt')
