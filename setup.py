from setuptools import setup, find_packages

setup(
    name='dask_accelerated',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'cython',
        'dask',
        'dask[distributed]'
        'dask[dataframe]',
        'pyarrow',
        'pandas',
        'xeger',
        'progressbar2',
        'matplotlib',
        'jupyter_contrib_nbextensions'
    ]
)