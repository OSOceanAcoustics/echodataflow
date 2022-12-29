from dateutil import parser
import os
import re
from typing import Dict, Any, Literal, List
from zipfile import ZipFile

import fsspec

from ..utils import extract_fs

TRANSECT_FILE_REGEX = r"x(?P<transect_num>\d+)"


def parse_file_path(raw_file: str, fname_pattern: str) -> Dict[str, Any]:
    """
    Parses file path to get at the datetime

    Parameters
    ----------
    raw_file : str
        Raw file url string
    fname_pattern : str
        Regex pattern string for date extraction from file

    Returns
    -------
    dict
        Raw file url dictionary that contains parsed dates
    """
    matcher = re.compile(fname_pattern)
    file_match = matcher.search(raw_file)
    match_dict = file_match.groupdict()
    file_datetime = None
    if "date" in match_dict and "time" in match_dict:
        datetime_obj = parser.parse(
            f"{file_match['date']}{file_match['time']}"
        )  # noqa
        file_datetime = datetime_obj.isoformat()
        jday = datetime_obj.timetuple().tm_yday
        match_dict.pop("date")
        match_dict.pop("time")
        match_dict.setdefault("month", datetime_obj.month)
        match_dict.setdefault("year", datetime_obj.year)
        match_dict.setdefault("jday", jday)

    match_dict.setdefault("datetime", file_datetime)
    return dict(**match_dict)


def _extract_from_zip(
    file_system, file_path: str
) -> Dict[str, Dict[str, Any]]:
    transect_dict = {}
    """Extracts raw files from transect file zip format"""
    with file_system.open(file_path, 'rb') as f:
        with ZipFile(f) as zf:
            zip_infos = zf.infolist()
            for zi in zip_infos:
                m = re.match(TRANSECT_FILE_REGEX, zi.filename)
                transect_num = int(m.groupdict()['transect_num'])
                if zi.is_dir():
                    raise ValueError(
                        "Directory found in zip file. This is not allowed!"
                    )
                with zf.open(zi.filename) as txtfile:
                    file_list = txtfile.read().decode('utf-8').split('\r\n')
                    for rawfile in file_list:
                        transect_dict.setdefault(
                            rawfile,
                            {"filename": zi.filename, "num": transect_num},
                        )
    return transect_dict


def _extract_from_text(
    file_system, file_path: str
) -> Dict[str, Dict[str, Any]]:
    """Extracts raw files from transect file text format"""
    filename = os.path.basename(file_path)
    m = re.match(TRANSECT_FILE_REGEX, filename)
    transect_num = int(m.groupdict()['transect_num'])

    transect_dict = {}

    with file_system.open(file_path) as txtfile:
        file_list = txtfile.read().decode('utf-8').split('\r\n')
        for rawfile in file_list:
            transect_dict.setdefault(
                rawfile,
                {"filename": filename, "num": transect_num},
            )

    return transect_dict


def extract_transect_files(
    file_format: Literal['txt', 'zip'],
    file_path: str,
    storage_options: Dict[str, Any] = {},
) -> Dict[str, Dict[str, Any]]:
    """
    Extracts raw file names and transect number from transect file(s)

    Parameters
    ----------
    file_format : str
        Transect file format. Options are 'txt' or 'zip' only.
    file_path : str
        The full path to the transect file.
    storage_options : dict
        Storage options for transect file access.

    Returns
    -------
    dict
        Raw file name dictionary with the original transect file name
        and transect number. For example:

        ```
        {
            'Summer2017-D20170627-T171516.raw': {
                'filename': 'x0007_fileset.txt',
                'num': 7
            },
            'Summer2017-D20170627-T174838.raw': {
                'filename': 'x0007_fileset.txt',
                'num': 7
            }
        }
        ```

    """
    file_system = extract_fs(file_path, storage_options=storage_options)
    if file_format == "zip":
        return _extract_from_zip(file_system, file_path)
    elif file_format == "txt":
        return _extract_from_text(file_system, file_path)
    else:
        raise ValueError(
            f"Invalid file format: {file_format}. Only 'txt' or 'zip' are valid"
        )
