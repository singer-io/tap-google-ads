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

        # call_details, campaign_labels do not have an id field
        # accessible_bidding_strategies, accounts, user_list, and bidding_strategies streams have only 1 record.
        # So, skipped those streams.
        streams_to_test = {'ad_groups': 132093547633, 'ads': 586722860308, 'campaign_budgets': 10006446850,
                           'campaigns': 15481829265, 'labels': 21628120997, 'carrier_constant': 70094, 'feed': 351805305,
                           'feed_item': 216977537909, 'language_constant': 1007, 'mobile_app_category_constant': 60009,
                           'mobile_device_constant': 604043, 'operating_system_version_constant': 630166, 'topic_constant': 41,
                           'user_interest': 959, 'campaign_criterion': 16990616126, 'ad_group_criterion': 131901833709}

        for stream, last_pk_fetched in streams_to_test.items():
            self.run_test(stream, last_pk_fetched)

    def run_test(self, stream, last_pk_fetched):
        """
        Scenario: A sync job is interrupted for full table stream. The state is saved with `currently_syncing`
                    and `last_pk_fetched`(id of last synced record).
                  The next sync job kicks off, the tap picks only remaining records for interrupted stream and complete the sync.

        Test Cases:
            - Verify id value of each record in interrupted sync is less than last_pk_fetched(id of last synced record).
            - Verify that all records in the full sync and interrupted sync come in Ascending order.
            - Verify interrupted_sync has the fewer records as compared to full sync
            - Verify state is flushed if sync is completed.
        """

        # Create connection using a recent start date
        conn_id = connections.ensure_connection(self)

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # partition catalogs for use in table/field seelction
        test_catalogs = [catalog for catalog in found_catalogs
                           if catalog.get('stream_name') in {stream}]

        # select fields
        self.select_all_streams_and_fields(conn_id, test_catalogs, select_all_fields=False)

        # Run a sync
        self.run_and_verify_sync(conn_id)

        # acquire records from target output
        full_sync_records = runner.get_records_from_target_output()

        interrupted_state = {
            'currently_syncing': (stream, '5548074409'),
            'bookmarks': {
                stream: {'5548074409': {'last_pk_fetched': last_pk_fetched}},
           }
        }

        # set state for interrupted sync
        menagerie.set_state(conn_id, interrupted_state)

        # Run another sync
        self.run_and_verify_sync(conn_id)

        # acquire records from target output
        interrupted_sync_records = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)

        with self.subTest(stream=stream):

            # gather results
            full_records = [message['data'] for message in full_sync_records[stream]['messages']]
            full_record_count = len(full_records)
            interrupted_records = [message['data'] for message in interrupted_sync_records[stream]['messages']]
            interrupted_record_count = len(interrupted_records)

            # Verify second sync(interrupted_sync) have the less records as compared to first sync(full sync)
            self.assertLess(interrupted_record_count, full_record_count)

            # campaign_criterion and ad_group_criterion streams have a composite primary key.
            # But, to filter out the records, we are using only campaign_id and ad_group_id respectively.
            if stream == "campaign_criterion":
                primary_key = "campaign_id"
            elif stream == "ad_group_criterion":
                primary_key = "ad_group_id"
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

            if stream in ["campaign_criterion", "ad_group_criterion"]:
                # Above streams have composite primary key.
                # These streams fetch records that have an id greater or equal to last_pk_fetched.
                # Verify id of 1st record in interrupted sync is greater than or equal to last_pk_fetched(id of last synced record).
                self.assertGreaterEqual(interrupted_records[0][primary_key], last_pk_fetched, msg='id of each record in interrupted sync is less than last_pk_fetched')
            else:
                # Verify id of 1st record in interrupted sync is greater than last_pk_fetched(id of last synced record).
                self.assertGreater(interrupted_records[0][primary_key], last_pk_fetched, msg='id of each record in interrupted sync is less than last_pk_fetched')

            # Verify state is flushed after sync completed.
            self.assertNotIn(stream, final_state['bookmarks'].keys())
