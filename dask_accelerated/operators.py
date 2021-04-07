import pyarrow
import pandas
import time
from dask.dataframe.accessor import maybe_wrap_pandas
from dask_accelerated import dask_native


class CustomFilter:

    def __init__(self):
        self.durations = []

    # The default str match operator in Dask, but this version keeps track
    # of str match duration
    def custom_vanilla(self, obj, accessor, attr, args, kwargs):

        start = time.time()
        out = getattr(getattr(obj, accessor, obj), attr)(*args, **kwargs)
        end = time.time()

        self.durations.append(end-start)

        return maybe_wrap_pandas(obj, out)

    # Performs string matching using Google's native RE2 library
    def custom_re2(self, obj, accessor, attr, args, kwargs):

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
        start = time.time()
        dask_native.re2Eval(
            number_of_records,
            regex,
            in_buffers[1].address,
            in_buffers[2].address,
            in_buffers[1].size,
            in_buffers[2].size,
            out_buffers[1].address,
            out_buffers[1].size
        )
        end = time.time()

        # Reconstruct output selection vector from the underlying buffers
        out = pyarrow.Array.from_buffers(pyarrow.bool_(), number_of_records, out_buffers).to_pandas()

        self.durations.append(end-start)

        return maybe_wrap_pandas(obj, out)

    def custom_re2_unaligned(self, obj, accessor, attr, args, kwargs):

        # The number of records in the current batch
        number_of_records = obj.size

        # The regular expression to be matched
        regex = args[0]

        # Add some padding to the pandas series, which will be removed from the buffers later
        obj_with_padding = pandas.concat([pandas.Series(["a"]), obj])
        arr = pyarrow.Array.from_pandas(obj_with_padding)
        in_buffers_with_padding = arr.buffers()

        # Remove padding from buffers to make them unaligned
        unaligned_offset = in_buffers_with_padding[1].slice(offset=4)
        unaligned_value = in_buffers_with_padding[2].slice(offset=1)

        # Verify that the buffers are indeed not aligned to 64 bytes
        offset_alignment = unaligned_offset.address % 64
        value_alignment = unaligned_value.address % 64
        assert 0 not in {offset_alignment, value_alignment}

        # Initialize an empty selection vector and extract it's arrow buffers
        out = pandas.array([False] * number_of_records, dtype=bool, copy=False)
        out_buffers = pyarrow.Array.from_pandas(out).buffers()

        # Do a native evaluation of the regex matching
        start = time.time()
        dask_native.re2Eval(
            number_of_records,
            regex,
            unaligned_offset.address,
            unaligned_value.address,
            unaligned_offset.size,
            unaligned_value.size,
            out_buffers[1].address,
            out_buffers[1].size
        )
        end = time.time()

        # Reconstruct output selection vector from the underlying buffers
        out = pyarrow.Array.from_buffers(pyarrow.bool_(), number_of_records, out_buffers).to_pandas()

        self.durations.append(end-start)

        return maybe_wrap_pandas(obj, out)

    # Performs string matching using Tidre on FPGA
    def custom_tidre(self, obj, accessor, attr, args, kwargs):
        # The number of records in the current batch
        number_of_records = obj.size

        # Extract arrow buffers from int input pandas series
        arr = pyarrow.Array.from_pandas(obj)
        in_buffers = arr.buffers()

        # Initialize an empty selection vector and extract it's arrow buffers
        out = pandas.array([False] * number_of_records, dtype=bool, copy=False)
        out_buffers = pyarrow.Array.from_pandas(out).buffers()

        # Do a native evaluation of the regex matching
        start = time.time()
        dask_native.tidreEval(
            number_of_records,
            in_buffers[1].address,
            in_buffers[2].address,
            in_buffers[1].size,
            in_buffers[2].size,
            out_buffers[1].address,
            out_buffers[1].size
        )
        end = time.time()

        # Reconstruct output selection vector from the underlying buffers
        out = pyarrow.Array.from_buffers(pyarrow.bool_(), number_of_records, out_buffers).to_pandas()

        self.durations.append(end - start)

        return maybe_wrap_pandas(obj, out)

    def custom_tidre_unaligned(self, obj, accessor, attr, args, kwargs):

        # The number of records in the current batch
        number_of_records = obj.size

        # The regular expression to be matched
        regex = args[0]

        # Add some padding to the pandas series, which will be removed from the buffers later
        obj_with_padding = pandas.concat([pandas.Series(["a"]), obj])
        arr = pyarrow.Array.from_pandas(obj_with_padding)
        in_buffers_with_padding = arr.buffers()

        # Remove padding from buffers to make them unaligned
        unaligned_offset = in_buffers_with_padding[1].slice(offset=4)
        unaligned_value = in_buffers_with_padding[2].slice(offset=1)

        # Verify that the buffers are indeed not aligned to 64 bytes
        offset_alignment = unaligned_offset.address % 64
        value_alignment = unaligned_value.address % 64
        assert 0 not in {offset_alignment, value_alignment}

        # Initialize an empty selection vector and extract it's arrow buffers
        out = pandas.array([False] * number_of_records, dtype=bool, copy=False)
        out_buffers = pyarrow.Array.from_pandas(out).buffers()

        # Do a native evaluation of the regex matching
        start = time.time()
        dask_native.tidreEval(
            number_of_records,
            regex,
            unaligned_offset.address,
            unaligned_value.address,
            unaligned_offset.size,
            unaligned_value.size,
            out_buffers[1].address,
            out_buffers[1].size
        )
        end = time.time()

        # Reconstruct output selection vector from the underlying buffers
        out = pyarrow.Array.from_buffers(pyarrow.bool_(), number_of_records, out_buffers).to_pandas()

        self.durations.append(end-start)

        return maybe_wrap_pandas(obj, out)
