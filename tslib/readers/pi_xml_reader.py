# coding=utf-8

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

    def bulk_get_series(self, chunk_size=75000):
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
        bulk_data = {
            "code": [],
            "name": [],
            "comment": [],
            "datetime": [],
            "flag_source": [],
            "flag": [],
            "location_code": [],
            "pru": [],
            "unit": [],
            "user": [],
            "value": [],
            "last_value_decimal": [],
        }
        meta_data = []

        for _, series in etree.iterparse(self.source, tag=SERIES):
            header = xmltodict.parse(etree.tostring(series[0]))['header']
            series_code = get_code(header)
            miss_val = header['missVal']

            meta_data.append({
                "code": series_code,
                "location_code": header['locationId'],
                "pru": header['parameterId'],
                "unit": header.get('units', None),
                "name": header['parameterId'],
                "location_name": header.get('stationName', '')[:80]
            })

            for event in series.iterchildren(tag=EVENT):
                # create timestamp and code rows, these will form the index
                d = event.attrib['date']
                t = event.attrib['time']
                bulk_data["datetime"].append(
                    parse_datetime("{}T{}".format(d, t)))
                bulk_data["code"].append(series_code)

                # add the data
                value = event.attrib['value']
                bulk_data["value"].append(
                    value if value != miss_val else "NaN")
                bulk_data["flag"].append(event.attrib.get('flag', None))
                bulk_data["flag_source"].append(
                    event.attrib.get('flagSource', None))
                bulk_data["comment"].append(event.attrib.get('comment', None))
                bulk_data["user"].append(event.attrib.get('user', None))

            while len(bulk_data["value"]) >= chunk_size:
                # Construct a pandas DataFrame from the events.

                # NB: np.float is shorthand for np.float64. This matches the
                # "double" type of the "value" attribute in the XML Schema
                # (an IEEE double-precision 64-bit floating-point number).
                dataframe = take_dataframe_from_bulk(bulk_data, chunk_size)

                if series.getparent()[0].tag == TIMEZONE:
                    offset = float(series.getparent()[0].text or 0)
                    tz_localize(dataframe, offset, copy=False)

                if series[-1].tag == COMMENT:
                    comment = series[-1]
                    if comment.text is not None:
                        meta_data[-1][u'comment'] = unicode(comment.text)

                series.clear()

                yield pd.DataFrame(meta_data), dataframe

                # Return empty metadata if all data was yielded or else the
                # last metadata.
                meta_data = [] if len(bulk_data["value"]) else meta_data[-1:]

        if bulk_data["value"]:
            # There is still some data left smaller than the chunk size. We
            # don't need to take care of cleaning up.
            dataframe = take_dataframe_from_bulk(bulk_data, chunk_size)
            yield pd.DataFrame(meta_data), dataframe


def take_dataframe_from_bulk(bulk_data, chunk_size):
    """
    Create a Timeseries dataframe with a maximum of chunk_size rows.

    Args:
        bulk_data(dict):
        chunk_size(int): max number of rows for the

    Returns:
        data_frame

    """
    data = {}

    def set_data(**columns_and_dtypes):
        for column, dtype in columns_and_dtypes.items():
            data[column] = np.array(
                bulk_data[column][:chunk_size], dtype=dtype)
            bulk_data[column] = bulk_data[column][chunk_size:]

    def set_if_any(**columns_and_dtypes):
        set_data(**{
            column: dtype for column, dtype in columns_and_dtypes.items()
            if any(bulk_data[column])
        })

    set_data(value=np.float, code=str)
    set_if_any(flag=np.int32, flag_source=str, comment=str, user=str)

    dataframe = pd.DataFrame(
        data=data, index=bulk_data["datetime"][:chunk_size])
    bulk_data["datetime"] = bulk_data["datetime"][chunk_size:]
    return dataframe


# TODO: this is an actual copy of the hydra-core method. Move it to this lib.
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
