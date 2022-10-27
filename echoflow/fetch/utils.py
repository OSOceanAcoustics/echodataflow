import re
from dateutil import parser


def parse_file_path(raw_file, fname_pattern):
    """Parses file path to get at the datetime"""
    matcher = re.compile(fname_pattern)
    file_match = matcher.search(raw_file)
    match_dict = file_match.groupdict()
    file_datetime = None
    if 'date' in match_dict and 'time' in match_dict:
        datetime_obj = parser.parse(
            f"{file_match['date']}{file_match['time']}"
        )
        file_datetime = datetime_obj.isoformat()
        jday = datetime_obj.timetuple().tm_yday
        match_dict.pop('date')
        match_dict.pop('time')
        match_dict.setdefault('month', datetime_obj.month)
        match_dict.setdefault('year', datetime_obj.year)
        match_dict.setdefault('jday', jday)

    match_dict.setdefault('datetime', file_datetime)
    return dict(**match_dict)
