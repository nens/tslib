from .ts_reader import TimeSeriesReader
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pycassa.cassandra.ttypes import NotFoundException 
import json
import pandas as pd
import pycassa


class BucketSize:
    HOURLY = 4
    DAILY = 3
    MONTHLY = 2
    YEARLY = 1


def bucket_size(sensor_id):
    return BucketSize.MONTHLY


def bucket_format(bucketsize):
    if (bucketsize == BucketSize.HOURLY):
        return '%Y-%m-%dT%H'
    if (bucketsize == BucketSize.DAILY):
        return '%Y-%m-%d'
    if (bucketsize == BucketSize.MONTHLY):
        return '%Y-%m'
    if (bucketsize == BucketSize.YEARLY):
        return '%Y'


def bucket_delta(bucketsize):
    if (bucketsize == BucketSize.HOURLY):
        return relativedelta( hours = +1 )
    if (bucketsize == BucketSize.DAILY):
        return relativedelta( days = +1 )
    if (bucketsize == BucketSize.MONTHLY):
        return relativedelta( months = +1 )
    if (bucketsize == BucketSize.YEARLY):
        return relativedelta( years = +1 )


def bucket_start(timestamp, bucketsize):
    if (bucketsize == BucketSize.HOURLY):
        return timestamp.replace(minute=0, second=0, microsecond=0)
    if (bucketsize == BucketSize.DAILY):
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    if (bucketsize == BucketSize.MONTHLY):
        return timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if (bucketsize == BucketSize.YEARLY):
        return timestamp.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)


class CassandraReader(TimeSeriesReader):

    def __init__(self, nodes, keyspace, column_family):
        pool = pycassa.ConnectionPool(keyspace=keyspace, server_list=nodes,
            prefill=False)
        self.cf = pycassa.ColumnFamily(pool, column_family)

    def read(self, sensor_id, start, end, params=[]):
        # The bucket size defines how much data is on one Cassandra row.
        bucket = bucket_size(sensor_id)

        key_format = sensor_id + ":" + bucket_format(bucket)
        stamp = bucket_start(start, bucket)
        delta = bucket_delta(bucket)

        colname_format = '%Y-%m-%dT%H:%M:%S'
        col_start = start.strftime(colname_format)
        col_end = end.strftime(colname_format)

        datetimes = {}
        data = {}

        # From each bucket within in the specified range, get the columns
        # within the specified range.
        while stamp < end:
            try:
                chunk = self.cf.get(stamp.strftime(key_format),
                    column_start=col_start, column_finish=col_end)
                for col_name in chunk:
                    dt = col_name[:19]
                    key = col_name[20:]
                    if (key in params) or (len(params) == 0):
                        if not dt in datetimes.keys():
                            datetimes[dt] = {}
                            datetimes[dt][key] = chunk[col_name]
                            if not key in data.keys():
                                data[key] = []
            except NotFoundException:
                pass
            stamp += delta
        # Now, reform the data by series.
        for dt in sorted(datetimes):
            for key in data:
                if key in datetimes[dt]:
                    data[key].append(datetimes[dt][key])
                else:
                    data[key].append(None)
        # And create the Pandas DataFrame.
        return pd.DataFrame(data=data, index=sorted(datetimes))
