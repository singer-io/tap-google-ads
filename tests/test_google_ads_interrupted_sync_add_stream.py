import os
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class InterruptedSyncAddStreamTest(GoogleAdsBase):
    """Test tap's ability to recover from an interrupted sync"""

    @staticmethod
    def name():
        return "tt_google_ads_interruption_add"

    def get_properties(self, original: bool = True):
        """Configurable properties, with a switch to override the 'start_date' property"""
        return_value = {
            'start_date':   '2022-01-22T00:00:00Z',
            'user_id':      'not used?',
            'customer_ids': ','.join(self.get_customer_ids()),
            'login_customer_ids': [{"customerId": os.getenv('TAP_GOOGLE_ADS_CUSTOMER_ID'),
                                    "loginCustomerId": os.getenv('TAP_GOOGLE_ADS_LOGIN_CUSTOMER_ID'),}],

        }

        if original:
            return return_value

        self.start_date = return_value["start_date"]
        return return_value


    def test_run(self):
        """
        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that `currently_syncing` stream.

        Expected State Structure:
            state = {'currently_syncing': ('<stream-name2>', '<customer-id>'),
            'bookmarks': {
                '<stream-name1>': {'<customer-id>': {'<replication-key>': <completed-bookmark-value>}},
                '<stream-name2>': {'<customer-id>': {'<replication-key>': <incomplete-bookmark-value>}},

        Test Cases:
         - Verify behavior is consistent when an added stream is selected between initial and resuming sync
         - Verify an interrupted sync can resume based on the `currently_syncing` and stream level bookmark value
         - Verify only records with replication-key values greater than or equal to the stream level bookmark are replicated on the resuming sync for the interrupted stream
         - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync. All yet-to-be-synced streams must replicate before streams that were already synced.
        """
        print("Interrupted Sync Test for tap-google-ads with added stream")

        # the following streams are under test as they all have 4 consecutive days with records e.g.
        # ('2022-01-23T00:00:00.000000Z', '2022-01-23T00:00:00.000000Z', '2022-01-24T00:00:00.000000Z', '2022-01-25T00:00:00.000000Z')])}
        streams_under_test = {'ads',
                              'account_performance_report',
                              'search_query_performance_report',
                              'user_location_performance_report',
                              'assets'
        }

        # Create connection using a recent start date
        conn_id = connections.ensure_connection(self, original_properties=False)

        # Run a discovery job
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id)

        # partition catalogs for use in table/field seelction
        test_catalogs_1 = [catalog for catalog in found_catalogs_1
                           if catalog.get('stream_name') in streams_under_test]
        core_catalogs_1 = [catalog for catalog in test_catalogs_1
                           if not self.is_report(catalog['stream_name'])]
        report_catalogs_1 = [catalog for catalog in test_catalogs_1
                             if self.is_report(catalog['stream_name'])]

        # select all fields for core streams
        self.select_all_streams_and_fields(conn_id, core_catalogs_1, select_all_fields=True)

        # select 'default' fields for report streams
        self.select_all_streams_and_default_fields(conn_id, report_catalogs_1)

        # Run a sync
        full_sync = self.run_and_verify_sync(conn_id)

        # acquire records from target output
        full_sync_records = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)

        # Add a stream between syncs
        added_stream = 'ad_performance_report'
        streams_under_test.add(added_stream)
        added_stream_catalog = [catalog for catalog in found_catalogs_1
                           if catalog.get('stream_name') == added_stream]

        # add new stream to selected list
        self.select_all_streams_and_default_fields(conn_id, added_stream_catalog)

        # NB | Set state such that all but two streams have 'completed' a sync. The final stream ('user_location_performance_report') should
        #      have no bookmark value while the interrupted stream ('search_query_performance_report') should have a bookmark value prior to the
        #      'completed' streams.
        #      (These dates are the most recent where data exists before and after the manipulated bookmarks for each stream.)
        completed_bookmark_value = '2022-01-24T00:00:00.000000Z'
        interrupted_bookmark_value = '2022-01-23T00:00:00.000000Z'
        interrupted_state = {
            'currently_syncing': ('search_query_performance_report', '5548074409'),
            'bookmarks': {
                'account_performance_report': {'5548074409': {'date': completed_bookmark_value}},
                'search_query_performance_report': {'5548074409': {'date': interrupted_bookmark_value}},
           },
         }

        menagerie.set_state(conn_id, interrupted_state)

        # Run another sync
        interrupted_sync = self.run_and_verify_sync(conn_id)

        # acquire records from target output
        interrupted_sync_records = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get('currently_syncing')

        # Checking resuming sync resulted in successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in state for sync 1
            self.assertIsNone(currently_syncing)

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state.get('bookmarks'))

        # stream-level assertions
        for stream in streams_under_test:
            with self.subTest(stream=stream):

                # set expectations
                expected_replication_method = self.expected_replication_method()[stream]
                conversion_window = timedelta(days=30) # defaulted value
                today_datetime = dt.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

                # gather results
                if stream != added_stream:
                    full_records = [message['data'] for message in full_sync_records[stream]['messages']]
                    full_record_count = len(full_records)
                interrupted_records = [message['data'] for message in interrupted_sync_records[stream]['messages']]
                interrupted_record_count = len(interrupted_records)

                if expected_replication_method == self.INCREMENTAL:

                    # gather expectations
                    expected_primary_key = list(self.expected_primary_keys()[stream])[0]
                    expected_replication_key = list(self.expected_replication_keys()[stream])[0]  # assumes 1 value
                    testable_customer_ids = set(self.get_customer_ids()) - {'2728292456'}
                    for customer in testable_customer_ids:
                        with self.subTest(customer_id=customer):

                            # gather results
                            start_date_datetime = dt.strptime(self.start_date, self.START_DATE_FORMAT)
                            oldest_record_datetime = dt.strptime(interrupted_records[0].get(expected_replication_key), self.REPLICATION_KEY_FORMAT)
                            final_stream_bookmark = final_state['bookmarks'][stream]
                            final_bookmark = final_stream_bookmark.get(customer, {}).get(expected_replication_key)
                            final_bookmark_datetime = dt.strptime(final_bookmark, self.REPLICATION_KEY_FORMAT)

                            # Verify final bookmark saved match formatting standards for resuming sync
                            self.assertIsNotNone(final_bookmark)
                            self.assertIsInstance(final_bookmark, str)
                            self.assertIsDateFormat(final_bookmark, self.REPLICATION_KEY_FORMAT)

                            if stream in full_sync_state['bookmarks'].keys():
                                full_sync_stream_bookmark = full_sync_state['bookmarks'][stream]
                                full_sync_bookmark = full_sync_stream_bookmark.get(customer, {}).get(expected_replication_key)
                                full_sync_bookmark_datetime = dt.strptime(full_sync_bookmark, self.REPLICATION_KEY_FORMAT)

                            if stream in interrupted_state['bookmarks'].keys():
                                interrupted_stream_bookmark = interrupted_state['bookmarks'][stream]
                                interrupted_bookmark = interrupted_stream_bookmark.get(customer, {}).get(expected_replication_key)
                                interrupted_bookmark_datetime = dt.strptime(interrupted_bookmark, self.REPLICATION_KEY_FORMAT)

                                # Verify resuming sync replicates records inclusively
                                # by comparing the replication key-values to the interrupted state.
                                self.assertEqual(oldest_record_datetime, interrupted_bookmark_datetime)

                                # Verify resuming sync only replicates records with replication key values greater or equal to
                                # the interrupted_state for streams that completed were replicated during the interrupted sync.
                                for record in interrupted_records:
                                    with self.subTest(record_primary_key=record[expected_primary_key]):
                                        rec_time = dt.strptime(record.get(expected_replication_key), self.REPLICATION_KEY_FORMAT)
                                        self.assertGreaterEqual(rec_time, interrupted_bookmark_datetime)

                                # Record count for all streams of interrupted sync match expectations
                                full_records_after_interrupted_bookmark = 0
                                for record in full_records:
                                    rec_time = dt.strptime(record.get(expected_replication_key), self.REPLICATION_KEY_FORMAT)

                                    if rec_time >= interrupted_bookmark_datetime:
                                        full_records_after_interrupted_bookmark += 1
                                self.assertEqual(full_records_after_interrupted_bookmark, len(interrupted_records), \
                                                 msg="Expected {} records in each sync".format(full_records_after_interrupted_bookmark))
                            else:

                                self.assertGreater(len(interrupted_records), 0)

                            if stream != added_stream:

                                # Verify state ends with the same value for common streams after both full and interrupted syncs
                                self.assertEqual(full_sync_bookmark_datetime, final_bookmark_datetime)

                                # Verify the interrupted sync replicates the expected record set
                                # All interrupted recs are in full recs
                                for record in interrupted_records:
                                    self.assertIn(record, full_records, msg='incremental table record in interrupted sync not found in full sync')

                            else:

                                # Verify resuming sync replicates records starting with start date for streams that were yet-to-be-synced
                                # by comparing the replication key-values to the interrupted state.
                                self.assertEqual(oldest_record_datetime, start_date_datetime)

                            # Verify the bookmark is set based on sync end date (today) for resuming sync
                            self.assertEqual(final_bookmark_datetime, today_datetime)


                elif expected_replication_method == self.FULL_TABLE:

                    # Verify full table streams do not save bookmarked values at the conclusion of a succesful sync
                    self.assertNotIn(stream, full_sync_state['bookmarks'].keys())
                    self.assertNotIn(stream, final_state['bookmarks'].keys())

                    # Verify first and second sync have the same records
                    self.assertEqual(full_record_count, interrupted_record_count)
                    for rec in interrupted_records:
                        self.assertIn(rec, full_records, msg='full table record in interrupted sync not found in full sync')

                # Verify at least 1 record was replicated for each stream
                self.assertGreater(interrupted_record_count, 0)

                print(f"{stream} resumed sync records replicated: {interrupted_record_count}")
