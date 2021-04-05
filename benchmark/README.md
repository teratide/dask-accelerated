# Benchmarking Dask Accelerated
Two benchmarks are provided for either implementation of *dask accelerated*. The first benchmarks the regex filter
runtimes under varying input sizes, while the second benchmarks these runtimes under varying batch sizes.

The benchmarks for RE2 compare the RE2 implementation with vanilla Dask, while the Tidre benchmarks compare Tidre with
both the RE2 implementation and vanilla Dask. Running the Tidre benchmarks will therefore also generate RE2 results.

These benchmarks can be run from the provided `main.py`, from which the usage can be found below.
```
    usage: main.py [-h] [--tidre]

    Benchmark the RE2 and Tidre implementations of dask_accelerated.
    
    optional arguments:
      -h, --help  show this help message and exit
      --tidre     run the Tidre benchmarks. (default: only run RE2)
```

Note that performing the Tidre benchmarks requires an FPGA accelerator to be available and properly configured.

## Results
The results of these benchmarks automatically get written to a data file in `../notebooks/`. This folder contains
a jupyter notebook `plots.ipynb` that, when run, generates all relevant plots and saves them in `../notebooks/plots`.