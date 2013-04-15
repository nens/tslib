import unittest

from tslib.writers.ts_writer import TimeSeriesWriter


class TestTimeSeriesWriter(unittest.TestCase):

    def test_set_series(self):
        """The set_series method must be overriden by subclasses."""
        writer = TimeSeriesWriter()
        self.assertRaises(NotImplementedError, writer.set_series, None, None)
