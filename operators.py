from dask.dataframe.accessor import maybe_wrap_pandas
# import dask_accelerated

def custom_regex(obj, accessor, attr, args, kwargs):
    # number_of_records = obj.size
    # regex = args[0]
    #
    # arr = pyarrow.Array.from_pandas(obj)
    # in_buffers = arr.buffers()

    # Original computation
    out = getattr(getattr(obj, accessor, obj), attr)(*args, **kwargs)

    # out = pandas.array([False] * number_of_records * 8, dtype=bool, copy=False)
    # out_buffers = pyarrow.Array.from_pandas(original_out).buffers()
    #
    # dask_accelerated.re2Eval(
    #     number_of_records,
    #     regex,
    #     in_buffers[1].address,
    #     in_buffers[2].address,
    #     in_buffers[1].size,
    #     in_buffers[2].size,
    #     out_buffers[1].address,
    #     out_buffers[1].size
    # )
    #
    # out = pyarrow.Array.from_buffers(pyarrow.bool_(), number_of_records, out_buffers).to_pandas()
    #
    # orig = maybe_wrap_pandas(obj, original_out)
    # new = maybe_wrap_pandas(obj, out)

    return maybe_wrap_pandas(obj, out)