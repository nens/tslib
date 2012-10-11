class CassandraWriter(object):

    def __init__(self, cf):
        self.cf = cf

    def write(self, dfs):
        with self.cf.batch() as b:
            i = 0
            for df in dfs:
                i += 1
                key = 'sensor-%d-' % i
                key = key + '%Y-%m-%dT%H:%M:%S%z'
                for timestamp, row in df.iterrows():
                    b.insert(
                        timestamp.strftime(key),
                        dict((k, str(v)) for k, v in row.to_dict().iteritems())
                    )
