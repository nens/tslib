from .ts_reader import TimeSeriesReader
from datetime import datetime
from pytz import FixedOffset
import logging
import numpy as np
import pandas as pd
import xmltodict

logger = logging.getLogger(__name__)

try:
    # Fastest?
    from lxml import etree
    assert etree  # Silence pyflakes
    logger.info('Running with lxml.etree')
except ImportError:
    try:
        # Faster?
        import xml.etree.cElementTree as etree
        assert etree  # Silence pyflakes
        logger.info('Running with xml.etree.cElementTree')
    except ImportError:
        try:
            # Fast?
            import xml.etree.ElementTree as etree
            assert etree  # Silence pyflakes
            logger.info('Running with xml.etree.ElementTree')
        except:
            logger.critical('No ElementTree API available')

NS = 'http://www.wldelft.nl/fews/PI'
TIMEZONE = '{%s}timeZone' % NS
SERIES = '{%s}series' % NS


class PiXmlReader(TimeSeriesReader):
    """docstring"""

    def __init__(self, source):
        """docstring"""
        self.source = source

    def get_series(self):
        """docstring"""

        for _, series in etree.iterparse(self.source, tag=SERIES):

            iterator = series.iterchildren()

            header = iterator.next()
            metadata = xmltodict.parse(etree.tostring(header))['header']
            missVal = metadata['missVal']

            datetimes = []
            values = []
            flags = []
            comments = []
            users = []

            for event in iterator:
                date = event.attrib['date']
                time = event.attrib['time']
                datetime_str = "%sT%s" % (date, time)
                format_str = "%Y-%m-%dT%H:%M:%S"
                datetimes.append(datetime.strptime(datetime_str, format_str))
                value = event.attrib['value']
                values.append(value if value != missVal else "NaN")
                flags.append(event.attrib.get('flag', None))
                comments.append(event.attrib.get('comment', None))
                users.append(event.attrib.get('user', None))

            data = {'value': np.array(values, np.float)}

            if any(flags):
                data['flag'] = flags
            if any(comments):
                data['comment'] = comments
            if any(users):
                data['user'] = users

            dataframe = pd.DataFrame(data=data, index=datetimes)

            series.clear()

            if series.getparent()[0].tag == TIMEZONE:
                offset = float(series.getparent()[0].text or 0)
                tz_localize(dataframe, offset, copy=False)

            yield metadata, dataframe


def tz_localize(dataframe, offset_in_hours=0, copy=True):
    """Localize tz-naive TimeSeries to a fixed-offset time zone.

    Most time zones are offset from UTC by a whole number of hours,
    but a few are offset by 30 or 45 minutes. An offset of 0 is
    special-cased to return UTC.

    Parameters
    ----------
    dataframe: instance of pandas.DataFrame having a tz-naive DatetimeIndex.
    offset_in_hours : fixed offset in hours from UTC, e.g. -6 or +9.5.
    copy : boolean, default True - make a copy of the underlying data.

    Returns
    -------
    instance of pandas.DataFrame having a tz-aware DatetimeIndex.
    """
    return dataframe.tz_localize(FixedOffset(offset_in_hours * 60), copy=copy)
