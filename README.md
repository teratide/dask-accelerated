# Dask Accelerated
An accelerated version of Dask which substitutes operators in the Dask task graph with an accelerated version.
This new operator can do it's evaluation using native libraries or by offloading the computation to an FPGA accelerator.

## Installation

* Install Apache Arrow 3.0+
* Python 3.8+
* Pip/conda

Build the dask-accelerated binaries
```
    mkdir -p build
    cd build
    cmake ../native
    make
    cd ..
```

Install python dependencies

```
    pip install -e .
```

Build datasets

```
    python3 data-generator/main.py
```

Run dask-accelerated

```
    python3 dask-accelerated/main.py
```