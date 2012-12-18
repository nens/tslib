import os
import unittest

from tslib.readers import PiXmlReader

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


class TestPiXmlReader(unittest.TestCase):

    def test_parse_pi_xml_01(self):
        source = os.path.join(DATA_DIR, "time_series.xml")
        reader = PiXmlReader(source)
        for md, df in reader.get_series():
            pass
        self.assertTrue(True)

    def test_parse_pi_xml_02(self):
        """Parse a file having comment elements."""
        source = os.path.join(DATA_DIR, "GDresults_dam.xml")
        reader = PiXmlReader(source)
        for md, df in reader.get_series():
            pass
        self.assertTrue(True)
