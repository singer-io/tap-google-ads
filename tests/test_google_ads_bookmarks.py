"""Test tap discovery mode and metadata."""
import re
from datetime import datetime as dt

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class BookmarksTest(GoogleAdsBase):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tt_google_ads_bookmarks"

    def test_run(self):
        """
        Testing that basic sync functions without Critical Errors
        """
        print("Bookmarks Test for tap-google-ads")

        conn_id = connections.ensure_connection(self)

        # TODO we are only testing core strems at the moment
        streams_to_test = self.expected_streams() - {
            # 'account_performance_report',
            'ad_performance_report',
            'adgroup_performance_report',
            'age_range_performance_report',
            'audience_performance_report',
            'call_metrics_call_details_report',
            'campaign_performance_report',
            'click_performance_report',
            'display_keyword_performance_report',
            'display_topics_performance_report',
            'expanded_landing_page_report',
            'gender_performance_report',
            'geo_performance_report',
            'keywordless_query_report',
            'keywords_performance_report',
            'landing_page_report',
            'placeholder_feed_item_report',
            'placeholder_report',
            'placement_performance_report',
            'search_query_performance_report',
            'shopping_performance_report',
            'user_location_performance_report',
            'video_performance_report',
        }

        # Run a discovery job
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id)

        # partition catalogs for use in table/field seelction
        test_catalogs_1 = [catalog for catalog in found_catalogs_1
                           if catalog.get('stream_name') in streams_to_test]
        core_catalogs_1 = [catalog for catalog in test_catalogs_1
                           if not self.is_report(catalog['stream_name'])]
        report_catalogs_1 = [catalog for catalog in test_catalogs_1
                             if self.is_report(catalog['stream_name'])]

        # select all fields for core streams
        self.select_all_streams_and_fields(conn_id, core_catalogs_1, select_all_fields=True)

        # select 'default' fields for report streams
        self.select_all_streams_and_default_fields(conn_id, report_catalogs_1)

        # Run a sync 
        sync_job_name_1 = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status_1 = menagerie.get_exit_status(conn_id, sync_job_name_1)
        menagerie.verify_sync_exit_status(self, exit_status_1, sync_job_name_1)

        # acquire records from target output
        synced_records_1 = runner.get_records_from_target_output()
        state_1 = menagerie.get_state(conn_id)
        bookmarks_1 = state_1.get('bookmarks')
        currently_syncing_1 = state_1.get('currently_syncing', 'KEY NOT SAVED IN STATE')

        # TODO manipulate state dynamically
        manipulated_state = {'currently_syncing': 'None', 'bookmarks': {'account_performance_report': {'date': '2022-01-24T00:00:00.000000Z'}}}
        menagerie.set_state(conn_id, manipulated_state)

        # Run another sync
        sync_job_name_2 = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status_2 = menagerie.get_exit_status(conn_id, sync_job_name_2)
        menagerie.verify_sync_exit_status(self, exit_status_2, sync_job_name_2)

        # acquire records from target output
        synced_records_2 = runner.get_records_from_target_output()
        state_2 = menagerie.get_state(conn_id)
        bookmarks_2 = state_2.get('bookmarks')
        currently_syncing_2 = state_2.get('currently_syncing', 'KEY NOT SAVED IN STATE')

        # Checking syncs were successful prior to stream-level assertions
        with self.subTest():

            # BUG_TDL-17887 [tap-google-ads] State does not save `currently_syncing` as None when the sync successfully ends
            #               https://jira.talendforge.org/browse/TDL-17887
            
            # Verify sync is not interrupted by checking currently_syncing in state for sync 1
            # self.assertIsNone(currently_syncing_1) # BUG_TDL-17887
            # Verify bookmarks are saved
            self.assertIsNotNone(bookmarks_1)

            # Verify sync is not interrupted by checking currently_syncing in state for sync 2
            # self.assertIsNone(currently_syncing_2) # BUG_TDL-17887
            # Verify bookmarks are saved
            self.assertIsNotNone(bookmarks_2)

            # TODO only expected streams should have bookmarks ?

        # stream-level assertions
        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # set expectations
                expected_replication_method = self.expected_replication_method()[stream]

                # gather results
                records_1 = [message['data'] for message in synced_records_1[stream]['messages']]
                records_2 = [message['data'] for message in synced_records_2[stream]['messages']]
                record_count_1 = len(records_1)
                record_count_2 = len(records_2)
                stream_bookmark_1 = bookmarks_1.get(stream)
                stream_bookmark_2 = bookmarks_2.get(stream)

                # sanity check WIP
                print(f"Stream: {stream} \n"
                      f"Record 1 Sync 1: {records_1[0]}")
                # end WIP

                if expected_replication_method == self.INCREMENTAL:

                    # included to catch a contradiction in our base expectations
                    if not self.is_report(stream):
                        raise AssertionError(
                            f"Only Reports streams should be expected to support {expected_replication_method} replication."
                        )

                    # TODO need to finish implementing test cases for report streams
                    expected_replication_key = list(self.expected_replication_keys()[stream])[0]  # assumes 1 value

                    # Verify bookmarks saved match formatting standards for sync 1
                    self.assertIsNotNone(stream_bookmark_1)
                    bookmark_value_1 = stream_bookmark_1.get(expected_replication_key)
                    self.assertIsNotNone(bookmark_value_1)

                    self.assertIsInstance(bookmark_value_1, str)
                    try:
                        parsed_bookmark_value_1 = dt.strptime(bookmark_value_1, self.REPLICATION_KEY_FORMAT)
                    except ValueError as err:
                        raise AssertionError() from err

                    # # WIP 
                    # # Verify bookmarks saved match formatting standards for sync 2
                    # self.assertIsNotNone(stream_bookmark_2)
                    # bookmark_value_2 = stream_bookmark_2.get(expected_replication_key)
                    # self.assertIsNotNone(bookmark_value_2)
                    # self.assertIsInstance(bookmark_value_2, str)
                    # # BUG_TDL-17888  https://jira.talendforge.org/browse/TDL-17888
                    # # [tap-google-ads] Inconsistent bookmark value saved for `account_performance_report` stream
                    # self.assertIsInstance(bookmark_value_1, str)
                    # try:
                    #     parsed_bookmark_value_1 = dt.strptime(bookmark_value_1, self.REPLICATION_KEY_FORMAT)
                    # except ValueError as err:
                    #     raise AssertionError() from err


                    # TODO does this apply?
                    # Verify the bookmark is the max value sent to the target for the a given replication key.

                    # Verify 2nd sync only replicates records from the previous sync bookmark minus the conversion_window
                    # END WIP 
                    
                elif expected_replication_method == self.FULL_TABLE:

                    # Verify full table streams replicate the same number of records on each sync
                    self.assertEqual(record_count_1, record_count_2)

                    # Verify full table streams do not save bookmarked values at the conclusion of a succesful sync
                    self.assertIsNone(stream_bookmark_1)
                    self.assertIsNone(stream_bookmark_2)

                    # Verify full table streams replicate the same number of records on each sync
                    self.assertEqual(record_count_1, record_count_2)

                    # Verify full tables streams replicate the exact same set of records on each sync
                    for record in records_1:
                        self.assertIn(record, records_2)
                    
                # Verify at least 1 record was replicated for each stream
                self.assertGreater(record_count_1, 0)
                
                
                print(f"{stream} {record_count_1} records replicated.")
