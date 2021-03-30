import dask

# Returns a lazy result for the regex usecase in Dask
def get_lazy_result(in_size):

    # In size is in millions of records
    # Append 'M' to match the dataset filenames
    size = str(in_size) + "M"

    # Constants
    chunksize = 1e6
    parquet_engine = "pyarrow"  # Valid engines: ['fastparquet', 'pyarrow', 'pyarrow-dataset', 'pyarrow-legacy']
    file_root = "../data-generator/diving/data-"
    file_ext = ".parquet"
    regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'

    # Load the dataframe
    columns = ["value", "string"]
    filename = file_root + size + file_ext
    df = dask.dataframe.read_parquet(
        filename,
        chunksize=chunksize,
        engine=parquet_engine,
        gather_statistics=True,
        columns=columns
    )

    # Define the query using the pandas-like API
    return df[df["string"].str.match(regex)]["value"].sum()