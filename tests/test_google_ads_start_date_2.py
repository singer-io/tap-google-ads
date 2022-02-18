import os

from datetime import datetime as dt

from tap_tester import connections, runner, menagerie

from base import GoogleAdsBase
from test_google_ads_start_date import StartDateTest1


class StartDateTest2(StartDateTest1):

    def streams_to_test(self):
        return {'search_query_performance_report'}

    @staticmethod
    def name():
        return "tt_google_ads_start_date_2"

    def setUp(self):
        # TODO adjust these values
        #self.start_date_1 = self.get_properties().get('start_date') # '2021-12-01T00:00:00Z',
        self.start_date_1 = '2022-01-20T00:00:00Z' # '2022-01-25T00:00:00Z',
        self.start_date_2 = self.timedelta_formatted(self.start_date_1, days=2)
