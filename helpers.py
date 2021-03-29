import dask

def get_lazy_result(in_size):
    size = str(in_size) + "M"

    # Constants
    chunksize = 1e6
    parquet_engine = "pyarrow"
    file_root = "/home/bob/Documents/thesis/data/generator/diving/data-"
    file_ext = ".parquet"
    regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'

    # Register table
    filters = []
    columns = ["value", "string"]
    df = dask.dataframe.read_parquet(file_root + size + file_ext, chunksize=chunksize, engine=parquet_engine, gather_statistics=True, columns=columns)

    # Plan/optimize the query
    return df[df["string"].str.match(regex)]["value"].sum()