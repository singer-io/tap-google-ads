"""Test tap can handle running a sync with no streams selected."""
from tap_tester import menagerie, connections, runner, LOGGER

from base import GoogleAdsBase


class NoStreamsSelected(GoogleAdsBase):

    @staticmethod
    def name():
        return "tt_google_ads_no_streams"

    @staticmethod
    def streams_to_test():
        """No streams are selected."""
        return set()

    def test_run(self):
        """
        Verify tap can perform sync without Critical Error even if no streams are
        selected for replication.
        """

        LOGGER.info(
            "Field Exclusion Test with random field selection for tap-google-ads report streams.\n"
            f"Streams Under Test: {self.streams_to_test}"
        )

        conn_id = connections.ensure_connection(self)

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Run a sync job using orchestrator WITHOUT selecting streams
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target do not throw any errors
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify no records were replicated
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        self.assertEqual(sum(sync_record_count.values()), 0)

        # Verify state is empty
        state = menagerie.get_state(conn_id)
        state.pop('last_exception_triggered', None)
        self.assertDictEqual(dict(), state)
