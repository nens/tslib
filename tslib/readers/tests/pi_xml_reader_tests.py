import os
import unittest

from tslib.readers import PiXmlReader

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


class TestPiXmlReader(unittest.TestCase):

    def test_parse_pi_xml_01(self):
        """Parse a file."""
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

    def test_parse_pi_xml_03(self):
        """Parse a file with timeZone element."""
        source = os.path.join(DATA_DIR, "time_series.xml")
        reader = PiXmlReader(source)
        tz = reader.get_tz()
        self.assertEqual(1.0, tz)

    def test_parse_pi_xml_04(self):
        """Parse a file with empty timeZone element."""
        source = os.path.join(DATA_DIR, "empty_tz.xml")
        reader = PiXmlReader(source)
        tz = reader.get_tz()
        self.assertEqual(0.0, tz)

    def test_parse_pi_xml_05(self):
        """Parse a file without timeZone element."""
        source = os.path.join(DATA_DIR, "no_tz.xml")
        reader = PiXmlReader(source)
        tz = reader.get_tz()
        self.assertEqual(None, tz)

    def test_parse_pi_xml_06(self):
        """Parse a file without events ."""
        source = os.path.join(DATA_DIR, "no_events.xml")
        reader = PiXmlReader(source)
        for md, df in reader.get_series():
            self.assertEqual(None, df)

    def test_parse_pi_xml_07(self):
        """Parse a file."""
        source = os.path.join(DATA_DIR, "time_series.xml")
        reader = PiXmlReader(source)
        for md, df in reader.get_series():
            pass
        self.assertTrue(True)

    def test_parse_pi_xml_08(self):
        """Parse a file having comment elements."""
        source = os.path.join(DATA_DIR, "GDresults_dam.xml")
        reader = PiXmlReader(source)
        for md, df in reader.get_series():
            pass
        self.assertTrue(True)

    def test_parse_pi_xml_09(self):
        """Parse a file without events ."""
        source = os.path.join(DATA_DIR, "no_events.xml")
        reader = PiXmlReader(source)
        for md, df in reader.get_series():
            self.assertEqual(None, df)


class BulkTestPiXmlReader(unittest.TestCase):

    def test_parse_pi_xml_01(self):
        """Parse a file."""
        source = os.path.join(DATA_DIR, "time_series.xml")
        reader = PiXmlReader(source)
        for md, df in reader.bulk_get_series(chunk_size=5):
            pass
        self.assertTrue(True)

    def test_parse_pi_xml_02(self):
        """Parse a file having comment elements."""
        source = os.path.join(DATA_DIR, "GDresults_dam.xml")
        reader = PiXmlReader(source)
        for md, df in reader.bulk_get_series(chunk_size=5):
            pass
        self.assertTrue(True)

    def test_parse_pi_xml_03(self):
        """Parse a file with timeZone element."""
        source = os.path.join(DATA_DIR, "time_series.xml")
        reader = PiXmlReader(source)
        tz = reader.get_tz()
        self.assertEqual(1.0, tz)

    def test_parse_pi_xml_06(self):
        """Parse a file without events ."""
        source = os.path.join(DATA_DIR, "no_events.xml")
        reader = PiXmlReader(source)
        for md, df in reader.bulk_get_series(chunk_size=5):
            self.assertEqual(None, df)

    def test_parse_pi_xml_07(self):
        """Parse a file."""
        source = os.path.join(DATA_DIR, "time_series.xml")
        reader = PiXmlReader(source)
        for md, df in reader.bulk_get_series(chunk_size=300):
            pass
        self.assertTrue(True)

    def test_parse_pi_xml_08(self):
        """Parse a file having comment elements."""
        source = os.path.join(DATA_DIR, "GDresults_dam.xml")
        reader = PiXmlReader(source)
        for md, df in reader.bulk_get_series(chunk_size=5):
            pass
        self.assertTrue(True)

    def test_parse_pi_xml_09(self):
        """Parse a file without events ."""
        source = os.path.join(DATA_DIR, "no_events.xml")
        reader = PiXmlReader(source)
        for md, df in reader.bulk_get_series(chunk_size=5):
            self.assertEqual(None, df)
