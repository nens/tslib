tslib
=====

Yet another attempt to create a useful nens library for manipulating time series.

Building tslib
--------------
::
	git clone https://github.com/nens/tslib.git
	cd tslib
	python3 -m venv .venv
	. .venv/bin/activate
	pip install -r requirements-dev.txt

You can now run the tests::

	pytest
