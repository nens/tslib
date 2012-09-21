from .ts_reader import TimeSeriesReader
from datetime import datetime
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

        self.is_missVal = True if tag.endswith('missVal') else False

        if tag.endswith('series'):
            self.datetimes = []
            self.values = []
            self.flags = []
            self.comments = []
            self.users = []
        elif tag.endswith('event'):
            datetime_str = "%s %s" % (attrib['date'], attrib['time'])
            format_str = "%Y-%m-%d %H:%M:%S"
            self.datetimes.append(datetime.strptime(datetime_str, format_str))
            self.values.append(attrib['value']
                if attrib['value'] != self.metadata['missVal'] else "NaN")
            self.flags.append(attrib.get('flag', None))
            self.comments.append(attrib.get('comment', None))
            self.users.append(attrib.get('user', None))

    def end(self, tag):

        if tag.endswith('series'):
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
            df.metadata = self.metadata
            self.dfs.append(df)
        elif tag.endswith('missVal'):
            self.is_missVal = False

    def data(self, data):

        if self.is_missVal:
            self.metadata['missVal'] = data

    def close(self):
        """Returns a list of DataFrames.

        TODO: use a Panel instead?
        """

        return self.dfs


class PiXmlReader(TimeSeriesReader):

    @staticmethod
    def read(source):
        parser = etree.XMLParser(target=EventTarget())
        return etree.parse(source, parser)
