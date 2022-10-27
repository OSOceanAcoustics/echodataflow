from prefect import flow

from .tasks import parse_raw_json, data_convert


@flow
def conversion_pipeline(raw_url_file, deployment, client=None, config={}):
    """
    Conversion pipeline for raw echosounder data.
    The results will be converted as weekly files.

    Parameters
    ----------
    raw_url_file : str
        The path to raw urls json file
    config : dict
        Pipeline configuration file
    deployment : str
        The deployment string to identify combined file
    client : dask.distributed.Client, optional
        The dask client to use for `echopype.combine_echodata`

    Returns
    -------
    List of path string to converted files
    """
    all_weeks = parse_raw_json(raw_url_file)
    futures = []
    for idx, raw_dicts in enumerate(all_weeks):
        future = data_convert.submit(
            idx, raw_dicts, client, config, deployment
        )
        futures.append(future)
    return [f.result() for f in futures]
