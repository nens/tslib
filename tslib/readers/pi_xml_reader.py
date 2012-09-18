from .ts_reader import TimeSeriesReader
import logging

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
        self.values = []

    def start(self, tag, attrib):
        if tag.endswith('event'):
            value = attrib['value']
            self.values.append(value)

    def end(self, tag):
        pass

    def data(self, data):
        pass

    def close(self):
        return self.values


class PiXmlReader(TimeSeriesReader):

    @staticmethod
    def read(source):
        parser = etree.XMLParser(target=EventTarget())
        return etree.parse(source, parser)
