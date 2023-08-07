import dask.distributed

def get_client(client: dask.distributed.Client = None):
    """
    Task to get dask client if doesn't exist,
    otherwise use the client passed into argument

    Parameters
    ----------
    client : dask.distributed.Client, optional
        Dask distributed client object to use

    Returns
    -------
    dask.distributed.Client
    """
    if client is None:
        return dask.distributed.Client()
    return client