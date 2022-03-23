"""Test tap field exclusions with random field selection."""
from datetime import datetime as dt
from datetime import timedelta
import random

from tap_tester import menagerie, connections, runner

from base_google_ads_field_exclusion import FieldExclusionGoogleAdsBase


class FieldExclusion2(FieldExclusionGoogleAdsBase):

    @staticmethod
    def name():
        return "tt_google_ads_exclusion_2"

    streams_to_test = {
        "campaign_performance_report",
        # "click_performance_report", # NO EXCLUSIONS, SKIPPED INTENTIONALLY
        "display_keyword_performance_report",
        "display_topics_performance_report",
        "expanded_landing_page_report",
        "gender_performance_report",
     }

    def test_run(self):
        self.run_test()
