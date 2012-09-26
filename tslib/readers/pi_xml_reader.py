from .ts_reader import TimeSeriesReader
from datetime import datetime
from pytz import FixedOffset
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    # Fastest?
    from lxml import etree
    assert etree  # Silence pyflakes
    logger.debug('Running with lxml.etree')
except ImportError:
    try:
        # Faster?
        import xml.etree.cElementTree as etree
        assert etree  # Silence pyflakes
        logger.debug('Running with xml.etree.cElementTree')
    except ImportError:
        try:
            # Fast?
            import xml.etree.ElementTree as etree
            assert etree  # Silence pyflakes
            logger.debug('Running with xml.etree.ElementTree')
        except:
            logger.critical('No ElementTree API available')


def tz_localize(df, offset_in_hours=0, copy=True):
    """Localize tz-naive TimeSeries to a fixed-offset time zone.

    Most time zones are offset from UTC by a whole number of hours,
    but a few are offset by 30 or 45 minutes. An offset of 0 is
    special-cased to return UTC.

    Parameters
    ----------
    df : instance of pandas.DataFrame having a tz-naive DatetimeIndex.
    offset_in_hours : fixed offset in hours from UTC, e.g. -6 or +9.5.
    copy : boolean, default True - make a copy of the underlying data.

    Returns
    -------
    instance of pandas.DataFrame having a tz-aware DatetimeIndex.
    """
    return df.tz_localize(FixedOffset(offset_in_hours * 60), copy=copy)


class EventTarget(object):
    """SAX handler for parsing PI-XML timeseries.

    See: http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd

    TODO: timezones, metadata, and much more.
    """

    def __init__(self):

        self.metadata = {}
        self.datetimes = None
        self.values = None
        self.flags = None
        self.comments = None
        self.users = None
        self.dfs = []

    def start(self, tag, attrib):

        self.is_timeZone = True if tag.endswith('timeZone') else False
        self.is_missVal = True if tag.endswith('missVal') else False

        if tag.endswith('series'):
            self.datetimes = []
            self.values = []
            self.flags = []
            self.comments = []
            self.users = []
        elif tag.endswith('event'):
            # Pandas 0.8.1 has an issue with timezone-aware datetime objects:
            # ValueError: Tz-aware datetime.datetime cannot be converted to
            # datetime64 unless utc=True. Works in 0.9.0.dev-b9848a6.
            # Localizing naive TimeSeries afterwards is a strategy
            # that appears to work with 0.8.1 too.
            datetime_str = "%s %s" % (attrib['date'], attrib['time'])
            format_str = "%Y-%m-%d %H:%M:%S"
            self.datetimes.append(datetime.strptime(datetime_str, format_str))
            self.values.append(attrib['value']
                if attrib['value'] != self.metadata['missVal'] else "NaN")
            self.flags.append(attrib.get('flag', None))
            self.comments.append(attrib.get('comment', None))
            self.users.append(attrib.get('user', None))

    def end(self, tag):

        if tag.endswith('timeZone'):
            self.is_timeZone = False
            if not 'timeZone' in self.metadata:
                # The element is empty, so its default value applies.
                # For ease and reasons of performance, the default
                # value, 0.0, is not retrieved from the XSD.
                self.metadata['timeZone'] = 0.0
        elif tag.endswith('series'):
            data = {'value': np.array(self.values, np.float)}
            if any(self.flags):
                data['flag'] = self.flags
            if any(self.comments):
                data['comment'] = self.comments
            if any(self.users):
                data['user'] = self.users
            df = pd.DataFrame(
                data=data,
                index=self.datetimes
                )
            if 'timeZone' in self.metadata:
                tz_localize(df, self.metadata['timeZone'], copy=False)
            self.dfs.append(df)
        elif tag.endswith('missVal'):
            self.is_missVal = False

    def data(self, data):

        if self.is_timeZone:
            self.metadata['timeZone'] = float(data)
        elif self.is_missVal:
            self.metadata['missVal'] = data

    def close(self):
        """Returns a list of DataFrames.

        Why not return a Panel? Creating a Panel will not preserve the metadata
        on each DataFrame. Storing metadata on the Panel seems less convenient.
        """

        return self.dfs


class PiXmlReader(TimeSeriesReader):

    @staticmethod
    def read(source):
        parser = etree.XMLParser(target=EventTarget())
        return etree.parse(source, parser)
