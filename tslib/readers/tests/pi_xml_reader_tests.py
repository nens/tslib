from tslib.readers import PiXmlReader
import os
import unittest

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


class TestPiXmlReader(unittest.TestCase):

    def test_0010(self):
        source = os.path.join(DATA_DIR, "time_series.xml")
        PiXmlReader.read(source)
        self.assertTrue(True)
