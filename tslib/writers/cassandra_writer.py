import pycassa
import json


class CassandraWriter(object):

    def __init__(self, nodes, keyspace, column_family, queue_size):
        pool = pycassa.ConnectionPool(keyspace=keyspace, server_list=nodes,
            prefill=False)
        self.cf = pycassa.ColumnFamily(pool, column_family)
        self.qs = queue_size

    def write(self, dataframes):
        sensor_id = 'pixml-test'
        key_format = sensor_id + ':%Y-%m'
        colname_format = '%Y-%m-%dT%H:%M:%S%z'
        with self.cf.batch(queue_size=self.qs) as b:
            for df in dataframes:
                print json.dumps(df)
                for timestamp, row in df.iterrows():
                    b.insert(
                        timestamp.strftime(key_format),
                        dict(("%s_%s" % (timestamp.strftime(colname_format), k),
                              str(v)) for k, v in row.to_dict().iteritems())
                    )
