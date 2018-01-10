import unittest

from tslib.readers.ts_reader import TimeSeriesReader


class TestTimeSeriesReader(unittest.TestCase):

    def test_get_series(self):
        """The get_series method must be overriden by subclasses."""
        reader = TimeSeriesReader()
        self.assertRaises(NotImplementedError, reader.get_series)

    def test_bulk_get_series(self):
        """The get_series method must be overriden by subclasses."""
        reader = TimeSeriesReader()
        self.assertRaises(NotImplementedError, reader.bulk_get_series)
