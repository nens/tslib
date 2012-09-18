from .ts_reader import TimeSeriesReader
from datetime import datetime
import logging
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

    def __init__(self):
        self.datetimes = None
        self.values = None
        self.ts = []

    def start(self, tag, attrib):

        if tag.endswith('series'):
            self.datetimes = []
            self.values = []
            return

        if tag.endswith('event'):
            datetime_str = "%s %s" % (attrib['date'], attrib['time'])
            format_str = "%Y-%m-%d %H:%M:%S"
            self.datetimes.append(datetime.strptime(datetime_str, format_str))
            self.values.append(float(attrib['value']))
            return

    def end(self, tag):

        if tag.endswith('series'):
            self.ts.append(pd.Series(self.values, index=self.datetimes))

    def data(self, data):
        pass

    def close(self):
        return self.ts


class PiXmlReader(TimeSeriesReader):

    @staticmethod
    def read(source):
        parser = etree.XMLParser(target=EventTarget())
        return etree.parse(source, parser)
