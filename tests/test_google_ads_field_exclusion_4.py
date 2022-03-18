"""Test tap field exclusions with random field selection."""
from datetime import datetime as dt
from datetime import timedelta
import random

from tap_tester import menagerie, connections, runner

from base_google_ads_field_exclusion import FieldExclusionGoogleAdsBase


class FieldExclusion4(FieldExclusionGoogleAdsBase):

    @staticmethod
    def name():
        return "tt_google_ads_exclusion_4"

    streams_to_test = {
        "placement_performance_report",
        "search_query_performance_report",
        "shopping_performance_report",
        "user_location_performance_report",
        "user_location_performance_report",
        "video_performance_report",
     }

    def test_run(self):
        self.run_test()
