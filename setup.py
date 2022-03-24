#!/usr/bin/env python

from setuptools import setup

setup(name='tap-google-ads',
      version='0.3.0',
      description='Singer.io tap for extracting data from the Google Ads API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_google_ads'],
      install_requires=[
          'singer-python==5.12.2',
          'requests==2.26.0',
          'backoff==1.8.0',
          'google-ads==15.0.0',
          'protobuf==3.17.3',
      ],
      extras_require= {
          'dev': [
              'pylint',
              'nose',
              'ipdb',
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
