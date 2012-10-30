from .ts_writer import TimeSeriesWriter
from lxml import etree


DNS = 'http://www.wldelft.nl/fews/PI'  # default namespace
XSI = 'http://www.w3.org/2001/XMLSchema-instance'
XSD = 'http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd'

nsmap = {None: DNS, 'xsi': XSI}


class PiXmlWriter(TimeSeriesWriter):
    """docstring"""

    def __init__(self):
        """docstring"""
        self.root = etree.Element('TimeSeries', nsmap=nsmap)
        self.root.attrib['{%s}schemaLocation' % XSI] = "%s %s" % (DNS, XSD)
        self.root.attrib['version'] = '1.2'

    def set_series(self, metadata, dataframe):
        """docstring"""
        series = etree.SubElement(self.root, 'series')
        header = etree.SubElement(series, 'header')
        for row in dataframe.itertuples():
            event = etree.SubElement(series, 'event')

    def write(self, out, pretty_print=True):
        out.write(etree.tostring(self.root, pretty_print=pretty_print))
