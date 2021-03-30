import pyarrow
import pandas
from dask.dataframe.accessor import maybe_wrap_pandas
import dask_accelerated

# Performs string matching using Google's native RE2 library
def custom_re2(obj, accessor, attr, args, kwargs):

    # The number of records in the current batch
    number_of_records = obj.size

    # The regular expression to be matched
    regex = args[0]

    # Extract arrow buffers from int input pandas series
    arr = pyarrow.Array.from_pandas(obj)
    in_buffers = arr.buffers()

    # Initialize an empty selection vector and extract it's arrow buffers
    out = pandas.array([False] * number_of_records, dtype=bool, copy=False)
    out_buffers = pyarrow.Array.from_pandas(out).buffers()

    # Do a native evaluation of the regex matching
    dask_accelerated.re2Eval(
        number_of_records,
        regex,
        in_buffers[1].address,
        in_buffers[2].address,
        in_buffers[1].size,
        in_buffers[2].size,
        out_buffers[1].address,
        out_buffers[1].size
    )

    # Reconstruct output selection vector from the underlying buffers
    out = pyarrow.Array.from_buffers(pyarrow.bool_(), number_of_records, out_buffers).to_pandas()

    return maybe_wrap_pandas(obj, out)