#!/usr/bin/env python

from setuptools import setup

setup(name='tap-google-ads',
      version='1.7.0',
      description='Singer.io tap for extracting data from the Google Ads API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_google_ads'],
      install_requires=[
          'singer-python==6.0.0',
          'requests==2.26.0',
          'backoff==2.2.1',
          'google-ads==22.1.0',
          'protobuf==4.24.4',

          # Necessary to handle gRPC exceptions properly, documented
          # in an issue here: https://github.com/googleapis/python-api-core/issues/301
          'grpcio-status==1.44.0',
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
