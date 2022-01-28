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
            # TODO we are only testing core strems at the moment
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

        # Run a discovery job
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify a catalog was produced for each stream under test
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0)
        found_catalog_names = {found_catalog['stream_name'] for found_catalog in found_catalogs}
        self.assertSetEqual(streams_to_test, found_catalog_names)

        # Perform table and field selection
        self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=True)

        # Run a sync 
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # acquire records from target output
        synced_records = runner.get_records_from_target_output()

        # Verify at least 1 record was replicated for each stream
        for stream in streams_to_test:
            record_count = len(synced_records[stream]['messages'])

            self.assertGreater(record_count, 0)
