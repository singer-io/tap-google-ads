"""Test tap discovery mode and metadata."""
import re

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class AutomaticFieldsGoogleAds(GoogleAdsBase):
    """
    Test tap's sync mode can extract records for all streams
    with minimum field selection.
    """

    @staticmethod
    def name():
        return "tt_google_ads_auto_fields"

    def test_error_case(self):
        """
        Testing that basic sync with minimum field selection results in Critical Errors with clear message.
        """
        print("Automatic Fields Test for tap-google-ads report streams")

        # --- Test report streams throw an error --- #

        streams_to_test = {stream for stream in self.expected_streams()
                           if stream == "shopping_performance_report"} # All other reports have automatic_keys

        conn_id = connections.ensure_connection(self)

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                catalogs_to_test = [catalog
                                    for catalog in found_catalogs
                                    if catalog["stream_name"] == stream]

                # select all fields for core streams and...
                self.select_all_streams_and_fields(
                    conn_id,
                    catalogs_to_test,
                    select_all_fields=False
                )
                try:
                    # Run a sync
                    sync_job_name = runner.run_sync_mode(self, conn_id)

                    exit_status = menagerie.get_exit_status(conn_id, sync_job_name)

                    self.assertEqual(1, exit_status.get('tap_exit_status'))
                    self.assertEqual(0, exit_status.get('target_exit_status'))
                    self.assertEqual(0, exit_status.get('discovery_exit_status'))
                    self.assertIsNone(exit_status.get('check_exit_status'))

                    # Verify error message tells user they must select an attribute/metric for the invalid stream
                    self.assertIn(
                        "Please select at least one attribute and metric in order to replicate",
                        exit_status.get("tap_error_message")
                    )
                    self.assertIn(stream, exit_status.get("tap_error_message"))

                finally:
                    # deselect stream once it's been tested
                    self.deselect_streams(conn_id, catalogs_to_test)

    def test_happy_path(self):
        """
        Testing that basic sync with minimum field selection functions without Critical Errors
        """
        print("Automatic Fields Test for tap-google-ads core streams and most reports")

        # --- Start testing core streams --- #

        conn_id = connections.ensure_connection(self)

        streams_to_test = {stream for stream in self.expected_streams()
                           if stream not in {
                                   # no test data available, but can generate
                                   "keywords_performance_report",  # needs a Search Campaign (currently have none)
                                   'click_performance_report',  # only last 90 days returned
                                   'display_keyword_performance_report',  # Singer Display #2, Ad Group 2
                                   'display_topics_performance_report',  # Singer Display #2, Ad Group 2
                                   # audiences are unclear on how metrics fall into segments
                                   'ad_group_audience_performance_report',  # Singer Display #2/Singer Display, Ad Group 2 (maybe?)
                                   'campaign_audience_performance_report',  # Singer Display #2/Singer Display, Ad Group 2 (maybe?)
                                   # cannot generate test data
                                   "call_details", # need test call data before data will be returned
                                   "shopping_performance_report",  # need Shopping campaign type, and link to a store
                                   "shopping_performance_report", # No automatic keys for this report
                                   "video_performance_report",  # need a video to show
                                   'placement_performance_report',  # need an app to run javascript to trace conversions
                           }}

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Perform table and field selection...
        catalogs_to_test = [catalog for catalog in found_catalogs
                            if catalog['stream_name'] in streams_to_test]
        # select no fields for streams and rely on automatic metadata to ensure minimum selection
        self.select_all_streams_and_fields(conn_id, catalogs_to_test, select_all_fields=False)

        # Run a sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # acquire records from target output
        synced_messages = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # gather expectations
                expected_primary_keys = list(self.expected_primary_keys()[stream])
                expected_auto_fields = self.expected_automatic_fields()

                # gather results
                synced_records = [message for message in
                                  synced_messages.get(stream, {'messages':[]}).get('messages', [])
                                  if message['action'] == 'upsert']
                actual_primary_key_values = [tuple([record.get('data').get(expected_pk)
                                                    for expected_pk in expected_primary_keys])
                                             for record in synced_records]

                # Verify some record messages were synced
                self.assertGreater(len(synced_records), 0)

                # Verify that all replicated records have unique primary key values.
                self.assertCountEqual(actual_primary_key_values, set(actual_primary_key_values))

                # Verify that only the automatic fields are sent in records
                for record in synced_records:
                    with self.subTest(record=record['data']):
                        record_keys = set(record['data'].keys())
                        self.assertSetEqual(expected_auto_fields[stream], record_keys)
