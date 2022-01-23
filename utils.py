import datetime

import pandas as pd


def calc_freq(start, end):
    if 7 >= abs((end - start).days) > 1:
        return 'D'

    elif 8 < abs((end - start).days):
        return "W"
    else:
        return "H"


def split_dates(start, end, freq=None) -> [[], []]:
    if not isinstance(start, datetime.datetime):
        start = datetime.datetime.fromtimestamp(start)
    if not isinstance(end, datetime.datetime):
        end = datetime.datetime.fromtimestamp(end)
    freq = freq or calc_freq(start, end)
    s = pd.date_range(start=start, end=end, freq=freq, closed="left")
    if s.empty:
        raise Exception(f"please check start {start} and end {end} dates")
    e = (s + pd.to_timedelta(1, unit=freq))
    return list(zip((s.astype('int64') // 1e9).tolist(), (e.astype('int64') // 1e9).tolist()))
