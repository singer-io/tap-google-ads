import os
import unittest
from datetime import datetime as dt

from tap_tester import connections, runner, menagerie

from base import GoogleAdsBase
from test_google_ads_start_date import StartDateTest


class StartDateTest2(StartDateTest):

    def name(self):
        return "tt_google_ads_start_date_2"

    def setUp(self):
        self.start_date_1 = '2022-01-20T00:00:00Z' # '2022-01-25T00:00:00Z',
        self.start_date_2 = self.timedelta_formatted(self.start_date_1, days=2)
        self.streams_to_test = {'search_query_performance_report'}

    def test_run(self):
        self.run_test()
