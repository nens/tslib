# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans, see LICENSE.rst.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

from ciso8601 import parse_datetime
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


def fast_iterparse(source, **kwargs):
    """ A version of lxml.etree.iterparse that cleans up its own memory usage

    See also:
        https://www.ibm.com/developerworks/xml/library/x-hiperfparse/
    """
    for event, elem in etree.iterparse(source, **kwargs):
        yield event, elem

        # It's safe to call clear here because no descendants will be accessed
        elem.clear()

        # Also eliminate now-empty references from the root node to `elem`
        while elem.getprevious() is not None:
            del elem.getparent()[0]


class PiXmlReader(TimeSeriesReader):
    """Read a PI XML file.

    See: https://publicwiki.deltares.nl/display/FEWSDOC/Delft-
    Fews+Published+Interface+timeseries+Format+(PI)+Import

    Time series are returned as a Pandas dataframe and a
    dictionary containing metadata. An effort has been
    made to achieve a fair balance between speed and
    resource consumption.

    """

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
        """Return a (metadata, dataframe) tuple.

        Metadata is returned as a dict:
        https://github.com/martinblech/xmltodict
        http://www.xml.com/pub/a/2006/05/31/⏎
        converting-between-xml-and-json.html

        The dataframe's DatetimeIndex has the same time zone (offset)
        as the PI XML, which is generally not UTC. If you need UTC,
        the returned data frame can be converted as follows:

        df.tz_convert('UTC', copy=False)

        Caveat: the PI XML timeZone element is optional. In that
        case, the DatetimeIndex has no time zone information.

        """
        self._check_duplicates()

        for _, series in etree.iterparse(self.source, tag=SERIES):

            header = series[0]
            metadata = xmltodict.parse(etree.tostring(header))
            missVal = metadata['header']['missVal']

            datetimes = []
            values = []
            flags = []
            flag_sources = []
            comments = []
            users = []

            iterator = series.iterchildren(tag=EVENT)

            for event in iterator:
                d = event.attrib['date']
                t = event.attrib['time']
                datetimes.append(parse_datetime("{}T{}".format(d, t)))
                value = event.attrib['value']
                values.append(value if value != missVal else "NaN")
                flags.append(event.attrib.get('flag', None))
                flag_sources.append(event.attrib.get('flagSource', None))
                comments.append(event.attrib.get('comment', None))
                users.append(event.attrib.get('user', None))

            if values:

                # Construct a pandas DataFrame from the events.

                # NB: np.float is shorthand for np.float64. This matches the
                # "double" type of the "value" attribute in the XML Schema
                # (an IEEE double-precision 64-bit floating-point number).

                data = {'value': np.array(values, np.float)}

                # The "flag" attribute in the XML Schema is of type "int".
                # This corresponds to a signed 32-bit integer. NB: this
                # is not the same as the "integer" type, which is an
                # infinite set. TODO: should we bother casting or
                # leave flags as strings?

                if any(flags):
                    data['flag'] = flags

                # The other attributes are of type "string".

                if any(flag_sources):
                    data['flagSource'] = flag_sources
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

    def _check_duplicates(self):
        """
        Checks whether location_code and timeseries code duplicates exist.

        Raises ValueError when duplicates are found.
        """
        check_set = set()
        for series_i, (_, series) in enumerate(
                fast_iterparse(self.source, tag=SERIES)):
            header = xmltodict.parse(etree.tostring(series[0]))['header']
            k = (get_code(header), header['locationId'])
            if k in check_set:
                raise ValueError(
                    'PiXML import failed because of duplicate Timeseries for '
                    'timeseries_code "%s", location_code "%s" and file "%s".' %
                    (k[0], k[1], self.source))
            check_set.add(k)

    def bulk_get_series(self, chunk_size=250000):
        """Return a (metadata, dataframe) tuple.

        Metadata is returned as a dict:
        https://github.com/martinblech/xmltodict
        http://www.xml.com/pub/a/2006/05/31/⏎
        converting-between-xml-and-json.html

        The dataframe's DatetimeIndex has the same time zone (offset)
        as the PI XML, which is generally not UTC. If you need UTC,
        the returned data frame can be converted as follows:

        df.tz_convert('UTC', copy=False)

        Caveat: the PI XML timeZone element is optional. In that
        case, the DatetimeIndex has no time zone information.
        """
        self._check_duplicates()

        meta_data = []

        # initialize a counter to index into the 'bulk_data' array
        i = 0

        # by default, do not localize
        tz_offset = None

        for series_i, (_, series) in enumerate(
                fast_iterparse(self.source, tag=SERIES)):
            header = xmltodict.parse(etree.tostring(series[0]))['header']
            series_code = get_code(header)
            miss_val = header['missVal']
            location_code = header['locationId']

            # get the timezone offset, only for the first entry
            if series_i == 0 and series.getparent()[0].tag == TIMEZONE:
                tz_offset = FixedOffset(
                    float(series.getparent()[0].text or 0) * 60)

            if series[-1].tag == COMMENT:
                comment = series[-1].text
            else:
                comment = None

            meta_data.append({
                "code": series_code,
                "location_code": location_code,
                "pru": header['parameterId'],
                "unit": header.get('units', None),
                "name": header['parameterId'],
                "location_name": (header.get('stationName', '') or '')[:80],
                "lat": float(header.get('lat', np.nan)),
                "lon": float(header.get('lon', np.nan)),
                "comment": comment,
            })

            for event in series.iterchildren(tag=EVENT):
                if i == 0:
                    # the first in this chunk: init the bulk_data

                    # NB: np.float is shorthand for np.float64. This matches the
                    # "double" type of the "value" attribute in the XML Schema
                    # (an IEEE double-precision 64-bit floating-point number).
                    bulk_data = {
                        "code": np.empty(chunk_size, dtype=object),
                        "comment": np.empty(chunk_size, dtype=object),
                        "timestamp": np.empty(chunk_size,
                                              dtype='datetime64[ms]'),
                        "flag_source": np.empty(chunk_size, dtype=object),
                        # use float64 to allow np.nan values
                        "flag": np.empty(chunk_size, dtype=np.float64),
                        "location_code": np.empty(chunk_size, dtype=object),
                        "user": np.empty(chunk_size, dtype=object),
                        "value": np.empty(chunk_size, dtype=np.float64),
                    }

                    # check if we need the leftover metadata from the prev iter
                    if len(meta_data) > 0:
                        if meta_data[0]['code'] != series_code:
                            meta_data = meta_data[1:]

                # create timestamp and code rows, these will form the index
                d = event.attrib['date']
                t = event.attrib['time']
                bulk_data["timestamp"][i] = "{}T{}".format(d, t)
                bulk_data["code"][i] = series_code
                bulk_data["location_code"][i] = location_code

                # add the data
                value = event.attrib['value']
                bulk_data["value"][i] = value if value != miss_val else np.nan
                bulk_data["flag"][i] = event.attrib.get('flag', np.nan)
                bulk_data["flag_source"][i] = \
                    event.attrib.get('flagSource', None)
                bulk_data["comment"][i] = event.attrib.get('comment', None)
                bulk_data["user"][i] = event.attrib.get('user', None)

                i += 1
                if i >= chunk_size:
                    i = 0  # for next iter

                    # Construct a pandas DataFrame from the events.
                    dataframe = dataframe_from_bulk(bulk_data, tz_offset)

                    yield pd.DataFrame(meta_data), dataframe

                    # keep the last metadata entry
                    meta_data = meta_data[-1:]

        if i > 0:
            # There is still some data left smaller than the chunk size. We
            # don't need to take care of cleaning up.

            for key, value in bulk_data.items():
                bulk_data[key] = value[:i]

            dataframe = dataframe_from_bulk(bulk_data, tz_offset)
            yield pd.DataFrame(meta_data), dataframe


def dataframe_from_bulk(data, tz_offset):
    """
    Create a Timeseries dataframe from a dict of ndarrays.

    Args:
        bulk_data(dict):
        tz_offset: pandas Offset or None

    Returns:
        pandas DataFrame object
    """
    timestamp = pd.DatetimeIndex(data['timestamp'])
    if tz_offset is not None:
        timestamp = timestamp.tz_localize(tz_offset)

    index = pd.MultiIndex.from_arrays(
        arrays=[
            pd.Categorical(data['code']),
            pd.Categorical(data['location_code']),
            timestamp,
        ],
        names=['code', 'location_code', 'timestamp']
    )
    dataframe = pd.DataFrame(
        data={k: data[k] for k in data if k not in index.names},
        index=index
    )

    return dataframe


def get_code(header):
    """Construct an ID from a PI XML time series header.

    Per organisation, the ID is assumed to be unique for a given location.
    This format has been chosen in close consultation with FEWS experts.

    Internally, time series are identified by their UUID. FEWS,
    however, has no notion of UUIDs. Let's construct an ID
    similar to the one used by the DDSC project, because
    that one has proven useful so far. See:
    https://github.com/ddsc/ddsc-worker/blob/master/
    ddsc_worker/importer.py

    """
    code = '{}::{}::{}::{}'.format(
        header['parameterId'],
        header['timeStep']['@unit'],
        header['timeStep'].get('@divider', 1),
        header['timeStep'].get('@multiplier', 1)
    )
    return code


def tz_localize(dataframe, offset_in_hours=0, copy=True, level=None):
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
    return dataframe.tz_localize(
        FixedOffset(offset_in_hours * 60), copy=copy, level=level)
