import re
import os
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class InterruptedSyncTest(GoogleAdsBase):
    """Test tap's ability to recover from an interrupted sync"""

    @staticmethod
    def name():
        return "tt_google_ads_interruption"

    def get_properties(self, original: bool = True):
        """Configurable properties, with a switch to override the 'start_date' property"""
        return_value = {
            'start_date':   '2021-12-01T00:00:00Z',
            'user_id':      'not used?', # TODO ?
            'conversion_window': '1',  # days
            'customer_ids': ','.join(self.get_customer_ids()),
            'login_customer_ids': [{"customerId": os.getenv('TAP_GOOGLE_ADS_CUSTOMER_ID'),
                                    "loginCustomerId": os.getenv('TAP_GOOGLE_ADS_LOGIN_CUSTOMER_ID'),}],

        }

        # TODO_TDL-17911 Add a test around conversion_window_days
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value


    def assertIsDateFormat(self, value, str_format): # TODO needed in this test?
        """
        Assertion Method that verifies a string value is a formatted datetime with
        the specified format.
        """
        try:
            _ = dt.strptime(value, str_format)
        except ValueError as err:
            raise AssertionError(
                f"Value does not conform to expected format: {str_format}"
            ) from err


    def test_run(self):
        """
        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that `currently_syncing` stream. 

        Test Cases:
         - Verify an interrupted sync can resume based on the `currently_syncing` and stream level bookmark value
         - Verify only records with replication-key values greater than or equal to the stream level bookmark are replicated on the resuming sync for the interrupted stream
         - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync. All yet-to-be-synced streams must replicate before streams that were already synced.
        """
        print("Interrupted Sync Test for tap-google-ads")

        # the following streams are under test as they all have 4 consecutive days with records e.g.
        # ('2022-01-23T00:00:00.000000Z', '2022-01-23T00:00:00.000000Z', '2022-01-24T00:00:00.000000Z', '2022-01-25T00:00:00.000000Z')])}
        streams_under_test = {'account_performance_report',
                              'ad_group_performance_report',
                              'ad_performance_report',
                              'age_range_performance_report',
                              'campaign_performance_report',
                              'click_performance_report',
                              'expanded_landing_page_report',
                              'gender_performance_report',
                              'geo_performance_report',
                              'keywordless_query_report',
                              'landing_page_report',
                              'search_query_performance_report',
                              'user_location_performance_report',
        }

        # Create connection using a recent start date
        self.start_date = '2022-01-22T00:00:00Z'
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
                'ad_group_performance_report': {'5548074409': {'date': completed_bookmark_value}},
                'ad_performance_report': {'5548074409': {'date': completed_bookmark_value}},
                'age_range_performance_report': {'5548074409': {'date': completed_bookmark_value}},
                'campaign_performance_report': {'5548074409': {'date': completed_bookmark_value}},
                'click_performance_report': {'5548074409': {'date': completed_bookmark_value}},
                'expanded_landing_page_report': {'5548074409': {'date': completed_bookmark_value}},
                'gender_performance_report': {'5548074409': {'date': completed_bookmark_value}},
                'geo_performance_report': {'5548074409': {'date': completed_bookmark_value}},
                'keywordless_query_report': {'5548074409': {'date': completed_bookmark_value}},
                'landing_page_report': {'5548074409': {'date': completed_bookmark_value}},
                'search_query_performance_report': {'5548074409': {'date': interrupted_bookmark_value}},
           },
         }
        menagerie.set_state(conn_id, interrupted_state)

        # Run another sync
        _ = self.run_and_verify_sync(conn_id)

        # acquire records from target output
        synced_records = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get('currently_syncing', 'KEY NOT SAVED IN STATE')

        # Checking resuming sync resulted in successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in state for sync 1
            self.assertEqual([None, None], currently_syncing)

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
                records = [message['data'] for message in synced_records[stream]['messages']]
                record_count = len(records)
             
                if expected_replication_method == self.INCREMENTAL:

                    # gather expectations
                    expected_primary_key = list(self.expected_primary_keys()[stream])[0]
                    expected_replication_key = list(self.expected_replication_keys()[stream])[0]  # assumes 1 value
                    testable_customer_ids = set(self.get_customer_ids()) - {'2728292456'}
                    for customer in testable_customer_ids:
                        with self.subTest(customer_id=customer):

                            # gather results
                            start_date_datetime = dt.strptime(self.start_date, self.START_DATE_FORMAT)
                            oldest_record_datetime = dt.strptime(records[0].get(expected_replication_key), self.REPLICATION_KEY_FORMAT)
                            final_stream_bookmark = final_state['bookmarks'][stream]
                            final_bookmark = final_stream_bookmark.get(customer, {}).get(expected_replication_key)
                            final_bookmark_datetime = dt.strptime(final_bookmark, self.REPLICATION_KEY_FORMAT)

                            # Verify final bookmark saved match formatting standards for resuming sync
                            self.assertIsNotNone(final_bookmark)
                            self.assertIsInstance(final_bookmark, str)
                            self.assertIsDateFormat(final_bookmark, self.REPLICATION_KEY_FORMAT)

                            if stream in interrupted_state['bookmarks'].keys():

                                interrupted_stream_bookmark = interrupted_state['bookmarks'][stream]
                                interrupted_bookmark = interrupted_stream_bookmark.get(customer, {}).get(expected_replication_key)
                                interrupted_bookmark_datetime = dt.strptime(interrupted_bookmark, self.REPLICATION_KEY_FORMAT)

                                # Verify resuming sync replicates records inclusively
                                # by comparing the replication key-values to the interrupted state.
                                self.assertEqual(oldest_record_datetime, interrupted_bookmark_datetime)

                                # Verify resuming sync only replicates records with replication key values greater or equal to
                                # the interrupted_state for streams that completed were replicated during the interrupted sync.
                                for record in records:
                                    with self.subTest(record_primary_key=record[expected_primary_key]):
                                        rec_time = dt.strptime(record.get(expected_replication_key), self.REPLICATION_KEY_FORMAT)
                                        self.assertGreaterEqual(rec_time, interrupted_bookmark_datetime)

                            else:

                                # Verify resuming sync replicates records starting with start date for streams that were yet-to-be-synced
                                # by comparing the replication key-values to the interrupted state.
                                self.assertEqual(oldest_record_datetime, start_date_datetime)
                                    
                            # Verify the bookmark is set based on sync end date (today) for resuming sync
                            self.assertEqual(final_bookmark_datetime, today_datetime)


                elif expected_replication_method == self.FULL_TABLE:

                    # Verify full table streams do not save bookmarked values at the conclusion of a succesful sync
                    self.assertIsNone(stream_bookmark_1)
                    self.assertIsNone(stream_bookmark_2)

                # Verify at least 1 record was replicated for each stream
                self.assertGreater(record_count, 0)

                print(f"{stream} resumed sync records replicated: {record_count}")
