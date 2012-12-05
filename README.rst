tslib
=====

Yet another attempt to create a useful nens library for manipulating time series.

Building tslib
--------------

Since tslib is basically a wrapper around `pandas <http://pandas.pydata.org/>`_, which is built on top of `Numpy <http://numpy.scipy.org/>`_, this transitive dependency needs to be installed first. On Ubuntu, this is most easily done via::

	sudo apt-get install python-numpy

Instructions for other platforms may be found `here <http://www.scipy.org/Installing_SciPy/>`_.

To build pandas, we'll need Python header files (installing it via your package manager will probably not give you an up-to-date version, because, at the time of writing, pandas is being actively developed, with new releases every month). On Ubuntu::

	sudo apt-get install python-dev

The `zc.buildout <http://www.buildout.org/>`_ software, which is used for building tslib, has a dependency on the setuptools package, which provides manipulation facilities for Python eggs. It will automatically be installed upon bootstrapping if not already present, but may also be installed beforehand::

	sudo apt-get install python-setuptools

If all is well, tslib should now build smoothly::

	git clone https://github.com/nens/tslib.git
	cd tslib
	python bootstrap.py
	bin/buildout