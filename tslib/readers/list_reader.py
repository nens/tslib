# (c) Nelen & Schuurmans.  MIT licensed, see LICENSE.rst.
from __future__ import unicode_literals

from datetime import datetime

import numpy as np
import pandas as pd
import pytz

from tslib.readers.ts_reader import TimeSeriesReader

INTERNAL_TIMEZONE = pytz.UTC
COLNAME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class ListReader(TimeSeriesReader):
    """docstring"""

    def __init__(self, serieslist):
        """docstring"""
        self.serieslist = serieslist

    def get_series(self):
        """docstring
        """

        for series in self.serieslist:
            datetimes = []
            data = {}
            keys = []
            for event in series.get('events'):
                dt = datetime.strptime(event.get('datetime'), COLNAME_FORMAT)
                dt = INTERNAL_TIMEZONE.localize(dt)
                for key in event.keys():
                    if key != 'datetime':
                        if key not in keys:
                            keys.append(key)
                        if not dt in data.keys():
                            data[dt] = {}
                        data[dt][key] = event.get(key)

            # Flatten the dataset by key.
            # Missing values are converted to None.
            datetimes = (data.keys())
            data_flat = {key: [] for key in keys}
            for dt in datetimes:
                row = data[dt]
                for key in keys:
                    data_flat[key].append(row.get(key))

            dataframe = pd.DataFrame(data=data_flat, index=datetimes)

            yield series.get('uuid'), dataframe

