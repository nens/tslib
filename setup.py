from setuptools import setup

version = '0.0.5.dev0'

long_description = '\n\n'.join([
    open('README.rst').read(),
    open('CREDITS.rst').read(),
    open('CHANGES.rst').read(),
    ])

install_requires = [
    'ciso8601',
    'lxml',
    'numpy',
    'pandas',
    'pytz',
    'setuptools',
    'xmltodict',
    ],

tests_require = [
    ]

setup(name='tslib',
      version=version,
      description="A library for manipulating time series",
      long_description=long_description,
      # Get strings from http://www.python.org/pypi?%3Aaction=list_classifiers
      classifiers=['Programming Language :: Python'],
      keywords=[],
      author='Carsten Byrman',
      author_email='carsten.byrman@nelen-schuurmans.nl',
      url='http://www.nelen-schuurmans.nl',
      license='MIT',
      packages=['tslib'],
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      tests_require=tests_require,
      extras_require={'test': tests_require},
      entry_points={
          'console_scripts': [
          ]},
      )
