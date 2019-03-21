Changelog of tslib
==================


0.0.10 (unreleased)
-------------------

- Nothing changed yet.


0.0.9 (2019-03-21)
------------------

- Instead of breaking off an import when a duplicate timeseries is
  encountered, show a warning, ignore the duplicate, and continue.


0.0.8 (2019-01-31)
------------------

- Added check for double timeseries in PiXML on read.


0.0.7 (2018-07-25)
------------------

- Clear lxml memory usage after each iteration in `bulk_get_series`

- Used preinitialized numpy arrays instead of appending lists to record the
  data in `bulk_get_series`.


0.0.6 (2018-07-09)
------------------

- Added bulk_get_series method to PiXmlReader.


0.0.5 (2014-10-30)
------------------

- Nothing changed yet.


0.0.4 (2013-05-13)
------------------

- Switched to MIT license.
- Updated setup.py.


0.0.3 (2013-04-16)
------------------

- Updated README.txt.
- Implemented PiXmlReader and PiXmlWriter.
- Implemented ListReader.


0.0.2 (2012-10-12)
------------------

- Nothing changed yet.


0.0.1 (2012-10-11)
------------------

- Initial project structure created with nensskel 1.26.
