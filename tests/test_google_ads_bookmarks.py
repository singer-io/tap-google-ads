"""Test tap discovery mode and metadata."""
import re

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class DiscoveryTest(GoogleAdsBase):
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

        streams_to_test = self.expected_streams() - {
            # TODO we are only testing core strems at the moment
            'landing_page_report',
            'expanded_landing_page_report',
            'display_topics_performance_report',
            'call_metrics_call_details_report',
            'gender_performance_report',
            'search_query_performance_report',
            'placeholder_feed_item_report',
            'keywords_performance_report',
            'video_performance_report',
            'campaign_performance_report',
            'geo_performance_report',
            'placeholder_report',
            'placement_performance_report',
            'click_performance_report',
            'display_keyword_performance_report',
            'shopping_performance_report',
            'ad_performance_report',
            'age_range_performance_report',
            'keywordless_query_report',
            'account_performance_report',
            'adgroup_performance_report',
            'audience_performance_report',
        }

        # Run a discovery job
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify a catalog was produced for each stream under test
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0)
        found_catalog_names = {found_catalog['stream_name'] for found_catalog in found_catalogs}
        self.assertSetEqual(streams_to_test, found_catalog_names)

        # Perform table and field selection
        self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=True)


        # Run a sync 
        sync_job_name_1 = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status_1 = menagerie.get_exit_status(conn_id, sync_job_name_1)
        menagerie.verify_sync_exit_status(self, exit_status_1, sync_job_name_1)

        # acquire records from target output
        synced_records_1 = runner.get_records_from_target_output()
        state_1 = menagerie.get_state(conn_id)

        # Run another sync
        sync_job_name_2 = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status_2 = menagerie.get_exit_status(conn_id, sync_job_name_2)
        menagerie.verify_sync_exit_status(self, exit_status_2, sync_job_name_2)

        # acquire records from target output
        synced_records_2 = runner.get_records_from_target_output()
        state_2 = menagerie.get_state(conn_id)


        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # set expectations
                expected_replication_method = self.expected_replication_method()[stream]

                # gather results
                records_1 = [message['data'] for message in synced_records_1[stream]['messages']]
                records_2 = [message['data'] for message in synced_records_2[stream]['messages']]
                record_count_1 = len(records_1)
                record_count_2 = len(records_2)
                bookmarks_1 = state_1.get(stream)
                bookmarks_2 = state_2.get(stream)

                # sanity check WIP
                print(f"Stream: {stream} \n"
                      f"Record 1 Sync 1: {records_1[0]}")
                # end WIP

                if expected_replication_method == self.INCREMENTAL:

                    # included to catch a contradiction in our base expectations
                    if not stream.endswith('_report'):
                        raise AssertionError(
                            f"Only Reports streams should be expected to support {expected_replication_method} replication."
                        )

                    # TODO need to finish implementing test cases for report streams

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify full table streams replicate the same number of records on each sync
                    self.assertEqual(record_count_1, record_count_2)

                    # Verify full table streams do not save bookmarked values at the conclusion of a succesful sync
                    self.assertIsNone(bookmarks_1)
                    self.assertIsNone(bookmarks_2)

                    # Verify full table streams replicate the same number of records on each sync
                    self.assertEqual(record_count_1, record_count_2)

                    # Verify full tables streams replicate the exact same set of records on each sync
                    for record in records_1:
                        self.assertIn(record, records_2)
                    
                # Verify at least 1 record was replicated for each stream
                self.assertGreater(record_count_1, 0)
                
                
                print(f"{stream} {record_count_1} records replicated.")
