import re

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class SyncCanaryTest(GoogleAdsBase):
    """
    Test tap's sync mode can extract records for all streams
    with standard table and field selection.
    """

    @staticmethod
    def name():
        return "tt_google_ads_canary"

    def test_run(self):
        """
        Testing that basic sync functions without Critical Errors
        """
        print("Canary Sync Test for tap-google-ads")

        conn_id = connections.ensure_connection(self)

        streams_to_test = self.expected_streams() - {
            # TODO_TDL-17885 the following are not yet implemented
            'display_keyword_performance_report', # no test data available
            'display_topics_performance_report',  # no test data available
            'placement_performance_report',  # no test data available
            "keywords_performance_report",  # no test data available
            "video_performance_report",  # no test data available
            "shopping_performance_report",  # no test data available (need Shopping campaign type)
            'campaign_audience_performance_report', # no test data available
            'ad_group_audience_performance_report',  # Potential BUG see above
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
                record_count = len(synced_records.get(stream, {'messages': []})['messages'])
                self.assertGreater(record_count, 0)
                print(f"{record_count} {stream} record(s) replicated.")
