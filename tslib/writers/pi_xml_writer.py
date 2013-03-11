from lxml import etree
import pytz
import xmltodict

from .ts_writer import TimeSeriesWriter


DNS = 'http://www.wldelft.nl/fews/PI'  # default namespace
XSI = 'http://www.w3.org/2001/XMLSchema-instance'
XSD = 'http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd'

nsmap = {None: DNS, 'xsi': XSI}

DATE_FMT = '%Y-%m-%d'
TIME_FMT = '%H:%M:%S'


def set_datetime(md, df):
    md['header']['startDate']['@date'] = df.index[0].strftime(DATE_FMT)
    md['header']['startDate']['@time'] = df.index[0].strftime(TIME_FMT)
    md['header']['endDate']['@date'] = df.index[-1].strftime(DATE_FMT)
    md['header']['endDate']['@time'] = df.index[-1].strftime(TIME_FMT)


class PiXmlWriter(TimeSeriesWriter):
    """docstring"""

    def __init__(self, offset_in_hours=0.0):
        """docstring

        Keyword arguments:
        offset_in_hours -- fixed offset in hours from UTC (default 0.0)

        Most time zones are offset from UTC by a whole number of hours,
        but a few are offset by 30 or 45 minutes. Pass `None` to omit
        the optional timeZone element in the resulting xml.

        """
        self.root = etree.Element('TimeSeries', nsmap=nsmap)
        self.root.attrib['{%s}schemaLocation' % XSI] = "%s %s" % (DNS, XSD)
        self.root.attrib['version'] = '1.2'
        if offset_in_hours is not None:
            etree.SubElement(self.root, 'timeZone').text = str(offset_in_hours)
            self.tz = pytz.FixedOffset(offset_in_hours * 60)

    def set_series(self, metadata, dataframe):
        """docstring"""
        series = etree.SubElement(self.root, 'series')

        if not dataframe.empty:
            dataframe.tz_convert(self.tz, copy=False)
            set_datetime(metadata, dataframe)

        header = xmltodict.unparse(metadata)
        header = bytes(bytearray(header, encoding='utf-8'))
        header = etree.XML(header)
        series.append(header)

        if dataframe.empty:
            return

        for idx, row in dataframe.iterrows():
            event = etree.SubElement(series, 'event')
            event.attrib['date'] = idx.strftime(DATE_FMT)
            event.attrib['time'] = idx.strftime(TIME_FMT)
            for col in dataframe.columns.tolist():
                event.attrib[col] = str(row[col])

    def write(self, out, pretty_print=True):
        """docstring"""
        out.write(etree.tostring(self.root, pretty_print=pretty_print))
