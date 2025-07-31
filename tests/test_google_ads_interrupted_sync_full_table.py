from asyncio import streams
import os

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class InterruptedSyncFullTableTest(GoogleAdsBase):
    """Test tap's ability to recover from an interrupted sync for FULL Table stream"""

    @staticmethod
    def name():
        return "tt_google_ads_interruption_full_table"

    def test_run(self):

        """
        Scenario: A sync job is interrupted for full table stream. The state is saved with `currently_syncing`
                    and `last_pk_fetched`(id of last synced record).
                  The next sync job kicks off, the tap picks only remaining records for interrupted stream and complete the sync.

        Expected State Structure:
            state = {'currently_syncing': ('<stream-name2>', '<customer-id>'),
            'bookmarks': {
                '<stream-name2>': {'<customer-id>': {last_pk_fetched: <incomplete-bookmark-value>}},

        Test Cases:
            - Verify that id of 1st record in interrupted sync is greater than or equal to last_pk_fetched.
            - Verify that all records in the full sync and interrupted sync come in Ascending order.
            - Verify interrupted_sync has the fewer records as compared to full sync
            - Verify state is flushed if sync is completed.
            - Verify resuming sync replicates all records for streams that were yet-to-be-synced
        """

        streams_under_test = {
            'ads',
            'campaign_criterion',
            'assets'
        }
    
        # Create connection
        conn_id = connections.ensure_connection(self)

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # partition catalogs for use in table/field selection
        test_catalogs = [catalog for catalog in found_catalogs
                           if catalog.get('stream_name') in streams_under_test]

        # select fields
        self.select_all_streams_and_fields(conn_id, test_catalogs, select_all_fields=False)

        # Run a sync
        self.run_and_verify_sync(conn_id)

        # acquire records from target output
        full_sync_records = runner.get_records_from_target_output()


        # Set state such that first stream has 'completed' a sync. The interrupted stream ('campaign_criterion') 
        # should have a bookmark value prior to the 'completed' streams.

        interrupted_state = {
            'currently_syncing': ('campaign_criterion', '5548074409'),
            'bookmarks': {
                'campaign_criterion': {'5548074409': {'last_pk_fetched': 16990616126}},
           }
        }

        # set state for interrupted sync
        menagerie.set_state(conn_id, interrupted_state)

        # Run another sync
        self.run_and_verify_sync(conn_id)

        # acquire records from target output
        interrupted_sync_records = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)

        # stream-level assertions
        for stream in streams_under_test:
            with self.subTest(stream=stream):

                # gather results
                full_records = [message['data'] for message in full_sync_records[stream]['messages']]
                full_record_count = len(full_records)
                interrupted_records = [message['data'] for message in interrupted_sync_records[stream]['messages']]
                interrupted_record_count = len(interrupted_records)

                # campaign_criterion stream has a composite primary key.
                # But, to filter out the records, we are using only campaign_id respectively.
                if stream == "campaign_criterion":
                    primary_key = "campaign_id"
                else:
                    primary_key = next(iter(self.expected_primary_keys()[stream]))

                # Verify that all records in the full sync come in Ascending order.
                # That means id of current record is greater than id of previous record.
                for i in range(1, full_record_count):
                    self.assertGreaterEqual(full_records[i][primary_key], full_records[i-1][primary_key], 
                                            msg='id of the current record is less than the id of the previous record.')

                # Verify that all records in the interrupted sync come in Ascending order.
                # That means id of current record is greater than id of previous record.
                for i in range(1, interrupted_record_count):
                    self.assertGreaterEqual(interrupted_records[i][primary_key], interrupted_records[i-1][primary_key], 
                                            msg='id of the current record is less than the id of the previous record.')

                if stream in interrupted_state['bookmarks'].keys():
                    
                    # Verify second sync(interrupted_sync) have the less records as compared to first sync(full sync) for interrupted stream
                    self.assertLess(interrupted_record_count, full_record_count)
                    
                    # Verify that id of 1st record in interrupted sync is greater than or equal to last_pk_fetched for interrupted stream.
                    self.assertGreaterEqual(interrupted_records[0][primary_key], 16990616126, msg='id of first record in interrupted sync is less than last_pk_fetched')
                
                else:
                    # Verify resuming sync replicates all records for streams that were yet-to-be-synced

                    for record in interrupted_records:
                        with self.subTest(record_primary_key=record[primary_key]):
                            self.assertIn(record, full_records, msg='Unexpected record replicated in resuming sync.')
                    
                    for record in full_records:
                        with self.subTest(record_primary_key=record[primary_key]):
                            self.assertIn(record, interrupted_records, msg='Record missing from resuming sync.' )    


                # Verify state is flushed after sync completed.
                self.assertNotIn(stream, final_state['bookmarks'].keys())
