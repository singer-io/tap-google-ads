import re
import unittest

from tap_tester import menagerie, connections, runner
from tap_tester.logger import LOGGER

from base import GoogleAdsBase


class SyncCanaryTest(GoogleAdsBase):
    """
    Test tap's sync mode can extract records for all streams
    with standard table and field selection.
    """

    @staticmethod
    def name():
        return "tt_google_ads_canary"

    @unittest.skip("USED FOR MANUAL VERIFICATION OF TEST DATA ONLY")
    def test_run(self):
        """
        Testing that basic sync functions without Critical Errors for streams without test data
        that are not covered in other tests.

        Test Data available for the following report streams across the following dates (only the
        first and last date that data was generated is listed).

        $ jq 'select(.table_name | contains("report")) | .table_name,.messages[0].data.date,
              .messages[-1].data.date' /tmp/tap-tester-target-out.json
        "account_performance_report"
        "2021-12-06T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        "ad_group_performance_report"
        "2021-12-06T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        "ad_performance_report"
        "2021-12-06T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        "age_range_performance_report"
        "2021-12-06T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        "campaign_performance_report"
        "2021-12-06T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        "click_performance_report"
        "2021-12-30T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        "expanded_landing_page_report"
        "2021-12-06T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        "gender_performance_report"
        "2021-12-06T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        "geo_performance_report"
        "2021-12-06T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        "keywordless_query_report"
        "2022-01-20T00:00:00.000000Z"
        "2022-01-25T00:00:00.000000Z"
        "landing_page_report"
        "2021-12-06T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        "search_query_performance_report"
        "2022-01-20T00:00:00.000000Z"
        "2022-01-25T00:00:00.000000Z"
        "user_location_performance_report"
        "2021-12-06T00:00:00.000000Z"
        "2022-03-14T00:00:00.000000Z"
        """
        LOGGER.info("Canary Sync Test for tap-google-ads")

        conn_id = connections.ensure_connection(self)

        streams_to_test = - self.expected_streams() - {
            # no test data available, but can generate
            "call_details", # need test call data before data will be returned
            "click_performance_report",  # only last 90 days returned
            "display_keyword_performance_report",  # Singer Display #2, Ad Group 2
            "display_topics_performance_report",  # Singer Display #2, Ad Group 2
            "keywords_performance_report",  # needs a Search Campaign (currently have none)
            # audiences are unclear on how metrics fall into segments
            "ad_group_audience_performance_report",  # Singer Display #2/Singer Display, Ad Group 2 (maybe?)
            "campaign_audience_performance_report",  # Singer Display #2/Singer Display, Ad Group 2 (maybe?)
            # cannot generate test data
            "placement_performance_report",  # need an app to run javascript to trace conversions
            "shopping_performance_report",  # need Shopping campaign type, and link to a store
            "video_performance_report",  # need a video to show
        }

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        test_catalogs = [catalog for catalog in found_catalogs if catalog['stream_name'] in streams_to_test]

        # Perform table and field selection...
        core_catalogs = [catalog for catalog in test_catalogs
                         if not self.is_report(catalog['stream_name'])
                         or catalog['stream_name'] == 'click_performance_report']
        report_catalogs = [catalog for catalog in test_catalogs
                           if self.is_report(catalog['stream_name'])
                           and catalog['stream_name'] != 'click_performance_report']
        # select all fields for core streams and...
        self.select_all_streams_and_fields(conn_id, core_catalogs, select_all_fields=True)
        # select 'default' fields for report streams
        self.select_all_streams_and_default_fields(conn_id, report_catalogs)

        # Run a sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # acquire records from target output
        synced_records = runner.get_records_from_target_output()

        # Verify at least 1 record was replicated for each stream
        for stream in streams_to_test:
            with self.subTest(stream=stream):
                self.assertGreater(record_count, 0)
                record_count = len(synced_records.get(stream, {'messages': []})['messages'])
                LOGGER.info(f"{record_count} {stream} record(s) replicated.")
