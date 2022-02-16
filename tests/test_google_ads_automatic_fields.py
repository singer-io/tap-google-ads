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

    def test_run(self):
        """
        Testing that basic sync with minimum field selection functions without Critical Errors
        """
        print("Automatic Fields Test for tap-google-ads")

        conn_id = connections.ensure_connection(self)

        streams_to_test = {stream for stream in self.expected_streams()
                           if self.is_report(stream)}

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Perform table and field selection...
        catalogs_to_test = [catalog for catalog in found_catalogs
                            if catalog['stream_name'] in streams_to_test]
        # select all fields for core streams and...
        self.select_all_streams_and_fields(conn_id, catalogs_to_test, select_all_fields=False)

        # Run a sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # TODO BUG Tap does not save exit status messages (just code=1) in the case where a Critical Error occurs.

        # TODO BUG Tap allows for invalid selection and does not throw an error, but rather attempts to query.

        # Verify the tap and target do not throw a critical error
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # TODO write an assertion around the error message thrown for report streams


        # # acquire records from target output
        # synced_records = runner.get_records_from_target_output()

        # # Verify at least 1 record was replicated for each stream
        # for stream in streams_to_test:

        #     with self.subTest(stream=stream):
        #         record_count = len(synced_records.get(stream, {'messages': []})['messages'])
        #         self.assertGreater(record_count, 0)
        #         print(f"{record_count} {stream} record(s) replicated.")
