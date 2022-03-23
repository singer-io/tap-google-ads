"""Test tap field exclusions with random field selection."""
from datetime import datetime as dt
from datetime import timedelta
import random

from tap_tester import menagerie, connections, runner

from base_google_ads_field_exclusion import FieldExclusionGoogleAdsBase


class FieldExclusion1(FieldExclusionGoogleAdsBase):

    @staticmethod
    def name():
        return "tt_google_ads_exclusion_1"

    streams_to_test = {
        "account_performance_report",
        "ad_group_audience_performance_report",
        "ad_group_performance_report",
        "ad_performance_report",
        "age_range_performance_report",
        "campaign_audience_performance_report",
     }

    def test_run(self):
        self.run_test()
