"""Test tap bookmarks and converstion window."""
import os
import re
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class BookmarksTest(GoogleAdsBase):
    """Test tap bookmarks."""

    @staticmethod
    def name():
        return "tt_google_ads_bookmarks"

    def assertIsDateFormat(self, value, str_format):
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
        Testing that the tap sets and uses bookmarks correctly where
        state < (today - converstion window), therefore the state should be used
        on sync 2

        Note:
          TDL-17918 implemented tap-tester level conversion window testing.  Unit tests cover
          additional scenarios

        """
        print("Bookmarks Test for tap-google-ads")

        self.end_date = "2022-02-01T00:00:00Z"

        conn_id = connections.ensure_connection(self)

        streams_under_test = self.expected_streams() - {
            'ad_group_audience_performance_report',
            'call_details', # need test call data before data will be returned
            'campaign_audience_performance_report',
            'click_performance_report',  # only last 90 days returned
            'display_keyword_performance_report',
            'display_topics_performance_report',
            'keywords_performance_report',
            'placement_performance_report',
            'search_query_performance_report',
            'shopping_performance_report',
            'video_performance_report',
        }

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
        _ = self.run_and_verify_sync(conn_id)

        # acquire records from target output
        synced_records_1 = runner.get_records_from_target_output()
        state_1 = menagerie.get_state(conn_id)
        bookmarks_1 = state_1.get('bookmarks')
        currently_syncing_1 = state_1.get('currently_syncing')

        # inject a simulated state value for each report stream under test
        data_set_state_value_1 = '2022-01-24T00:00:00.000000Z'
        data_set_state_value_2 = '2021-12-30T00:00:00.000000Z'
        injected_state_by_stream = {
            'ad_group_performance_report': data_set_state_value_1,
            'geo_performance_report': data_set_state_value_1,
            'gender_performance_report': data_set_state_value_1,
            'age_range_performance_report': data_set_state_value_1,
            'account_performance_report': data_set_state_value_1,
            'campaign_performance_report': data_set_state_value_1,
            'ad_performance_report': data_set_state_value_1,
            'expanded_landing_page_report': data_set_state_value_1,
            'keywordless_query_report': data_set_state_value_1,
            'landing_page_report': data_set_state_value_1,
            'user_location_performance_report': data_set_state_value_1,
        }

        manipulated_state = {
            'bookmarks': {
                stream: {os.getenv('TAP_GOOGLE_ADS_CUSTOMER_ID'): {'date': injected_state_by_stream[stream]}}
                for stream in streams_under_test
                if self.is_report(stream)
            }
        }
        menagerie.set_state(conn_id, manipulated_state)

        # Run another sync
        _ = self.run_and_verify_sync(conn_id)

        # acquire records from target output
        synced_records_2 = runner.get_records_from_target_output()
        state_2 = menagerie.get_state(conn_id)
        bookmarks_2 = state_2.get('bookmarks')
        currently_syncing_2 = state_2.get('currently_syncing')

        # Checking syncs were successful prior to stream-level assertions
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in state for sync 1
            self.assertIsNone(currently_syncing_1)
            # Verify bookmarks are saved
            self.assertIsNotNone(bookmarks_1)

            # Verify sync is not interrupted by checking currently_syncing in state for sync 2
            self.assertIsNone(currently_syncing_2)
            # Verify bookmarks are saved
            self.assertIsNotNone(bookmarks_2)

            # Verify ONLY report streams under test have bookmark entries in state for sync 1
            expected_incremental_streams = {stream for stream in streams_under_test if self.is_report(stream)}
            unexpected_incremental_streams_1 = {stream for stream in bookmarks_1.keys()
                                                if stream not in expected_incremental_streams}
            self.assertSetEqual(set(), unexpected_incremental_streams_1)

            # Verify ONLY report streams under test have bookmark entries in state for sync 2
            unexpected_incremental_streams_2 = {stream for stream in bookmarks_2.keys()
                                                if stream not in expected_incremental_streams}
            self.assertSetEqual(set(), unexpected_incremental_streams_2)

        # stream-level assertions
        for stream in streams_under_test:
            with self.subTest(stream=stream):

                # set expectations
                expected_replication_method = self.expected_replication_method()[stream]
                conversion_window = timedelta(days=30) # defaulted value
                end_datetime = dt.strptime(self.end_date, self.START_DATE_FORMAT)
                # gather results
                records_1 = [message['data'] for message in synced_records_1[stream]['messages']]
                records_2 = [message['data'] for message in synced_records_2[stream]['messages']]
                record_count_1 = len(records_1)
                record_count_2 = len(records_2)
                stream_bookmark_1 = bookmarks_1.get(stream)
                stream_bookmark_2 = bookmarks_2.get(stream)

                if expected_replication_method == self.INCREMENTAL:

                    # gather expectations
                    expected_replication_key = list(self.expected_replication_keys()[stream])[0]  # assumes 1 value
                    testable_customer_ids = set(self.get_customer_ids()) - {'2728292456'}
                    for customer in testable_customer_ids:
                        with self.subTest(customer_id=customer):
                            manipulated_bookmark = manipulated_state['bookmarks'][stream]
                            manipulated_state_formatted = dt.strptime(
                                manipulated_bookmark.get(customer, {}).get(expected_replication_key),
                                self.REPLICATION_KEY_FORMAT
                            )

                            # Verify bookmarks saved match formatting standards for sync 1
                            self.assertIsNotNone(stream_bookmark_1)
                            bookmark_value_1 = stream_bookmark_1.get(customer, {}).get(expected_replication_key)
                            self.assertIsNotNone(bookmark_value_1)
                            self.assertIsInstance(bookmark_value_1, str)
                            self.assertIsDateFormat(bookmark_value_1, self.REPLICATION_KEY_FORMAT)

                            # Verify bookmarks saved match formatting standards for sync 2
                            self.assertIsNotNone(stream_bookmark_2)
                            bookmark_value_2 = stream_bookmark_2.get(customer, {}).get(expected_replication_key)
                            self.assertIsNotNone(bookmark_value_2)
                            self.assertIsInstance(bookmark_value_2, str)
                            self.assertIsDateFormat(bookmark_value_2, self.REPLICATION_KEY_FORMAT)

                            # Verify the bookmark is set based on sync end date for sync 1
                            # (The tap replicaates from the start date through to end date)
                            parsed_bookmark_value_1 = dt.strptime(bookmark_value_1, self.REPLICATION_KEY_FORMAT)
                            self.assertEqual(parsed_bookmark_value_1, end_datetime)

                            # Verify the bookmark is set based on sync execution time for sync 2
                            # (The tap replicaates from the manipulated state through to end date)
                            parsed_bookmark_value_2 = dt.strptime(bookmark_value_2, self.REPLICATION_KEY_FORMAT)
                            self.assertEqual(parsed_bookmark_value_2, end_datetime)

                            # Verify 2nd sync only replicates records newer than manipulated_state_formatted
                            for record in records_2:
                                rec_time = dt.strptime(record.get(expected_replication_key), self.REPLICATION_KEY_FORMAT)
                                self.assertGreaterEqual(rec_time, manipulated_state_formatted, \
                                    msg="record time cannot be less than reference time: {}".format(manipulated_state_formatted)
                                )

                            # Verify  the number of records in records_1 where sync >= manipulated_state_formatted
                            # matches the number of records in records_2
                            records_1_after_manipulated_bookmark = 0
                            for record in records_1:
                                rec_time = dt.strptime(record.get(expected_replication_key), self.REPLICATION_KEY_FORMAT)
                                if rec_time >= manipulated_state_formatted:
                                    records_1_after_manipulated_bookmark += 1
                            self.assertEqual(records_1_after_manipulated_bookmark, record_count_2, \
                                             msg="Expected {} records in each sync".format(records_1_after_manipulated_bookmark))

                elif expected_replication_method == self.FULL_TABLE:
                    # Verify full table streams replicate the same number of records on each sync
                    self.assertEqual(record_count_1, record_count_2)

                    # Verify full table streams do not save bookmarked values at the conclusion of a succesful sync
                    self.assertIsNone(stream_bookmark_1)
                    self.assertIsNone(stream_bookmark_2)

                    # Verify full tables streams replicate the exact same set of records on each sync
                    for record in records_1:
                        self.assertIn(record, records_2)

                # Verify at least 1 record was replicated for each stream
                self.assertGreater(record_count_1, 0)
                self.assertGreater(record_count_2, 0)


                print(f"{stream} sync 2 records replicated: {record_count_2}")
