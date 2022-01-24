"""Test tap discovery mode and metadata."""
import re

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class DiscoveryTest(GoogleAdsBase):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tt_google_ads_canary"

    def test_run(self):
        """
        Testing that basic sync functions without Critical Errors
        """
        print("Discovery Test for tap-google-ads")

        conn_id = connections.ensure_connection(self)

        streams_to_test = self.expected_streams() - {
            # BUG_2 | missing 
            'landing_page_report',
            'expanded_landing_page_report',
            'display_topics_performance_report',
            'call_metrics_call_details_report',
            'gender_performance_report',
            'search_query_performance_report',
            'placeholder_feed_item_report',
            'keywords_performance_report',
            'video_performance_report',
            'campaign_performance_report',
            'geo_performance_report',
            'placeholder_report',
            'placement_performance_report',
            'click_performance_report',
            'display_keyword_performance_report',
            'shopping_performance_report',
            'ad_performance_report',
            'age_range_performance_report',
            'keywordless_query_report',
            'account_performance_report',
            'adgroup_performance_report',
            'audience_performance_report',
        }

        # found_catalogs = self.run_and_verify_check_mode(conn_id) # TODO PUT BACK
        # TODO REMOVE FROM HERE
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0)
        found_catalog_names = {found_catalog['stream_name'] for found_catalog in found_catalogs}
        self.assertSetEqual(streams_to_test, found_catalog_names)
        self.select_all_streams_and_fields(conn_id, found_catalogs, True)
        # self.perform_and_verify_table_and_field_selection(conn_id, found_catalogs) # TODO fix this in base

        # Run a sync 
        record_count_by_stream = self.run_and_verify_sync(conn_id)
