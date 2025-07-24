"""Test tap field exclusions with random field selection."""
from datetime import datetime as dt
from datetime import timedelta
import random

from tap_tester import menagerie, connections, runner

from base_google_ads_field_exclusion import FieldExclusionGoogleAdsBase


class FieldExclusion3(FieldExclusionGoogleAdsBase):

    @staticmethod
    def name():
        return "tt_google_ads_exclusion_3"

    streams_to_test = {
        "geo_performance_report",
        "keywordless_query_report",
        "keywords_performance_report",
        "landing_page_report",
     }

    def test_run(self):
        self.run_test()
