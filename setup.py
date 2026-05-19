#!/usr/bin/env python

from setuptools import setup

setup(name='tap-google-ads',
      version='2.1.0',
      description='Singer.io tap for extracting data from the Google Ads API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_google_ads'],
      install_requires=[
          'singer-python==6.1.1',
          'requests==2.34.2',
          'backoff==2.2.1',
          'google-ads==30.1.0',
          'protobuf==7.34.1',
      ],
      extras_require= {
          'dev': [
              'pylint',
              'ipdb',
              'setuptools',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-google-ads=tap_google_ads:main
      ''',
      packages=['tap_google_ads'],
      package_data = {
      },
      include_package_data=True,
)
