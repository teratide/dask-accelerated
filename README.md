# Dask Accelerated

[![test](https://github.com/teratide/dask-accelerated/actions/workflows/test.yml/badge.svg)](https://github.com/teratide/dask-accelerated/actions/workflows/test.yml)
[![lint](https://github.com/teratide/dask-accelerated/actions/workflows/lint.yml/badge.svg)](https://github.com/teratide/dask-accelerated/actions/workflows/lint.yml)

An accelerated version of Dask which substitutes operators in the Dask task graph with an accelerated version.
This new operator can do it's evaluation using native libraries or by offloading the computation to an FPGA accelerator.

## Installation

### Requirements
* [Apache Arrow 3.0+](https://arrow.apache.org/install/)
* Cmake 3.16+
* Python 3.8+
* Pip

### 1: Clone the repository
Clone the repository including submodules
```
    git clone https://github.com/teratide/dask-accelerated.git --recursive
    cd dask-accelerated
```

### 2: Build binaries
Fletcher on its own has no platform support; support for specific FPGA platforms needs to be installed separately.
The support library for AWS can be found at `native/fletcher-aws/runtime/runtime`. Before we can build that, however,
we'll need some environment setup from AWS.
```
    source native/fletcher-aws/aws-fpga/sdk_setup.sh
```

After that, just install normally.
```
    cd native/fletcher-aws/runtime/runtime
    mkdir -p build
    cd build
    cmake3 ..
    make -j
    sudo make install
    sudo ldconfig
    cd ../../../../..
```

In order to communicate with the FPGA, we'll also need the XDMA driver. It can be installed as follows.
```
    cd native/fletcher-aws/aws-fpga/sdk/linux_kernel_drivers/xdma
    make -j
    sudo make install
    cd ../../../../../..
```

Now the *dask accelerated* native binaries can be build, these also build Fletcher and RE2.
```
    mkdir -p native/build
    cd native/build
    cmake3 ..
    make
    cd ../..
```

### 3: Configure FPGA
Before the FPGA can be configured with the correct bitstream, it needs to be cleared.
```
    sudo fpga-clear-local-image -S 0
```

The bitstream that performs the target regular expression evaluation can now be loaded.
A different bitsream is required if another regular expression needs to be evaluated on the FPGA.
```
    # Regex evaluation for `teratide diving subsurface`
    sudo fpga-load-local-image -S 0 -I agfi-028fcd53df79020d9
```

From this point onward, loading another bistream does not require clearing the FPGA.

### 4: Install python dependencies
A `setup.py` specifies the required modules, these can be installed using pip.

```
    pip3 install -e .
```

### 5: Build datasets (optional)
The benchmarks provided in `benchmarks/` automatically generate any missing datasets.
These datasets can also be generated in advances,
but this has no effect on the runtime results of the benchmarks themselves.

```
    cd data-generator
    python3 main.py
    cd ..
```

## Running dask accelerated
This section explains how to run *dask accelerated* from the command line. Alternatively,
[benchmarks](./benchmark/README.md) or [tests](./test/README.md) can be performed.

The usage of *dask accelerated* can be found below.
```
    usage: main.py [-h] [--vanilla] [--re2] [--tidre] N B
    
    Dask Accelerated.
    
    positional arguments:
      N           input size in records at which to run dask accelerated
      B           batch size in records at which to run dask accelerated, must be a multiple of 1000
    
    optional arguments:
      -h, --help  show this help message and exit
      --vanilla   run vanilla Dask. (default: do not run)
      --re2       run the RE2 implementation. (default: do not run)
      --tidre     run the Tidre implementation. (default: do not run)
```

For example, the following command can be used to run both the RE2 and Tidre implementations of *dask accelerated* on
a dataset of 2e6 rows with a batch size of 64e3.
```
    cd dask_accelerated
    python3 main.py 2e6 64e3 --re2 --tidre
```