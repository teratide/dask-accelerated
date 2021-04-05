# Testing Dask Accelerated
*Dask accelerated* makes use of python's build in `unittest`. The following command can be run from the root folder
to run all tests.
```
    python3 -m unittest
```

Note that this requires an FPGA accelerator to be available and properly configured.

Alternatively, separate tests (such as tests that do no require an FPGA and make use of the RE2 library) can be performed
by running a separate file in the `test/` folder.