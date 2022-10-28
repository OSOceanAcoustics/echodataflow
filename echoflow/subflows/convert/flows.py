from prefect import flow

from .tasks import parse_raw_json, data_convert, get_client


@flow
def conversion_pipeline(
    deployment, raw_dicts=[], raw_url_file=None, client=None, config={}
):
    """
    Conversion pipeline for raw echosounder data.
    The results will be converted as weekly files.

    Parameters
    ----------
    deployment : str
        The deployment string to identify combined file
    raw_dicts : list
        List of raw url dictionary
    raw_url_file : str, optional
        The path to raw urls json file
    config : dict
        Pipeline configuration file
    client : dask.distributed.Client, optional
        The dask client to use for `echopype.combine_echodata`

    Returns
    -------
    List of path string to converted files

    Notes
    -----
    Don't run this pipeline with Dask Task Runners
    """
    all_weeks = parse_raw_json(raw_dicts=raw_dicts, raw_url_file=raw_url_file)
    futures = []
    client = get_client(client)
    for idx, raw_dicts in enumerate(all_weeks):
        future = data_convert.submit(
            idx=idx,
            raw_dicts=raw_dicts,
            client=client,
            config=config,
            deployment=deployment,
        )
        futures.append(future)
    return [f.result() for f in futures]
