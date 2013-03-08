# coding=utf-8

from datetime import datetime
import logging

from lxml import etree
from pytz import FixedOffset
import numpy as np
import pandas as pd
import xmltodict

from tslib.readers.ts_reader import TimeSeriesReader

logger = logging.getLogger(__name__)

NS = 'http://www.wldelft.nl/fews/PI'
TIMEZONE = '{%s}timeZone' % NS
SERIES = '{%s}series' % NS
EVENT = '{%s}event' % NS
COMMENT = '{%s}comment' % NS


class PiXmlReader(TimeSeriesReader):
    """docstring"""

    def __init__(self, source):
        """docstring"""
        self.source = source

    def get_tz(self):
        """Return the offset in hours from UTC as a float.

        Note that pi xml deals with fixed offsets only. See:
        http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_sharedtypes.xsd

        A return value of None means that no `timeZone` element is present.
        An empty `timeZone` will return the default value, i.e. `0.0`.

        """
        for _, element in etree.iterparse(self.source):
            if element.tag == TIMEZONE:
                return float(element.text or 0.0)

    def get_series(self):
        """docstring

        Metadata is returned as a dict:
        https://github.com/martinblech/xmltodict
        http://www.xml.com/pub/a/2006/05/31/⏎
        converting-between-xml-and-json.html
        """

        for _, series in etree.iterparse(self.source, tag=SERIES):

            header = series[0]
            metadata = xmltodict.parse(etree.tostring(header))
            missVal = metadata['header']['missVal']

            datetimes = []
            values = []
            flags = []
            comments = []
            users = []

            iterator = series.iterchildren(tag=EVENT)

            for event in iterator:
                d = event.attrib['date']
                t = event.attrib['time']
                datetimes.append(datetime(
                    int(d[0:4]), int(d[5:7]), int(d[8:10]),
                    int(t[0:2]), int(t[3:5]), int(t[6:8])))
                value = event.attrib['value']
                values.append(value if value != missVal else "NaN")
                flags.append(event.attrib.get('flag', None))
                comments.append(event.attrib.get('comment', None))
                users.append(event.attrib.get('user', None))

            if values:

                # Construct a pandas DataFrame from the events.

                data = {'value': np.array(values, np.float)}

                if any(flags):
                    data['flag'] = flags
                if any(comments):
                    data['comment'] = comments
                if any(users):
                    data['user'] = users

                dataframe = pd.DataFrame(data=data, index=datetimes)

                if series.getparent()[0].tag == TIMEZONE:
                    offset = float(series.getparent()[0].text or 0)
                    tz_localize(dataframe, offset, copy=False)

            else:

                # No events. The `minOccurs` attribute of the
                # `event` element is 0, so this valid XML.

                dataframe = None

            if series[-1].tag == COMMENT:
                comment = series[-1]
                if comment.text is not None:
                    metadata[u'comment'] = unicode(comment.text)

            series.clear()

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
