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
                           if not self.is_report(stream)}

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Perform table and field selection...
        catalogs_to_test = [catalog for catalog in found_catalogs
                            if catalog['stream_name'] in streams_to_test]
        # select all fields for core streams and...
        self.select_all_streams_and_fields(conn_id, catalogs_to_test, select_all_fields=False)

        # Run a sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # BUG https://jira.talendforge.org/browse/TDL-17841
        #    Tap does not save exit status messages (just code=1) in the case where a Critical Error occurs.

        # BUG https://jira.talendforge.org/browse/TDL-17840
        # Tap allows for invalid selection and does not throw an error, but rather attempts to query.

        # Verify the tap and target do not throw a critical error
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # TODO write an assertion around the error message thrown for report streams
        # TODO Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods

        # acquire records from target output
        synced_records = runner.get_records_from_target_output()


        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # BUG TDL-17840
                # # Verify that only the automatic fields are sent to the target.
                expected_auto_fields = self.expected_automatic_fields()
                expected_primary_key = list(self.expected_primary_keys()[stream])[0]  # assumes no compound-pks
                self.assertEqual(len(self.expected_primary_keys()[stream]), 1, msg="Compound pk not supported")
                # for record in synced_records[stream]['messages']:

                #     record_primary_key_values = record['data'][expected_primary_key]
                #     record_keys = set(record['data'].keys())

                #     with self.subTest(primary_key=record_primary_key_values):
                #         self.assertSetEqual(expected_auto_fields[stream], record_keys)

                # Verify that all replicated records have unique primary key values.
                actual_pks = [row.get('data').get(expected_primary_key) for row in
                                      synced_records.get(stream, {'messages':[]}).get('messages', []) if row.get('data')]

                self.assertCountEqual(actual_pks, set(actual_pks))
