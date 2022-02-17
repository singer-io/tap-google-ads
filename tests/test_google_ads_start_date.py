import os

from datetime import datetime as dt

from tap_tester import connections, runner, menagerie

from base import GoogleAdsBase


class StartDateTest(GoogleAdsBase):

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tt_google_ads_start_date"

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""

        self.start_date_1 = self.get_properties().get('start_date') # '2021-12-01T00:00:00Z',
        self.start_date_2 = self.timedelta_formatted(self.start_date_1, days=15)

        self.start_date = self.start_date_1

        # BUG https://jira.talendforge.org/browse/TDL-17839
        #     [tap-google-ads] Most performance reports are not including 'date' in output file

        streams_to_test = self.expected_streams() - {  # end result
            'display_keyword_performance_report', # no test data available
            'display_topics_performance_report',  # no test data available
            'placement_performance_report',  # no test data available
            "keywords_performance_report",  # no test data available
            "keywordless_query_report",  # no test data available
            "video_performance_report",  # no test data available
            'audience_performance_report',
            "shopping_performance_report",
            'landing_page_report',
            'expanded_landing_page_report',
            'user_location_performance_report',
        }

        ##########################################################################
        ### Sync with Connection 1
        ##########################################################################

        # instantiate connection
        conn_id_1 = connections.ensure_connection(self)

        # run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # table and field selection
        test_catalogs_1 = [catalog for catalog in found_catalogs_1
                           if catalog.get('stream_name') in streams_to_test]
        core_catalogs_1 = [catalog for catalog in test_catalogs_1 if not self.is_report(catalog['stream_name'])]
        # select all fields for core streams and...
        self.select_all_streams_and_fields(conn_id_1, core_catalogs_1, select_all_fields=True)
        # select 'default' fields for report streams
        for report in self.expected_default_fields().keys():
            if report not in streams_to_test:
                continue
            catalog = [catalog for catalog in test_catalogs_1
                       if catalog['stream_name'] == report][0]
            schema_and_metadata = menagerie.get_annotated_schema(conn_id_1, catalog['stream_id'])
            metadata = schema_and_metadata['metadata']
            properties = {md['breadcrumb'][-1]
                          for md in metadata
                          if len(md['breadcrumb']) > 0 and md['breadcrumb'][0] == 'properties'}
            expected_fields = self.expected_default_fields()[catalog['stream_name']]
            self.assertTrue(expected_fields.issubset(properties),
                            msg=f"{report} missing {expected_fields.difference(properties)}")
            non_selected_properties = properties.difference(expected_fields)
            connections.select_catalog_and_fields_via_metadata(
                conn_id_1, catalog, schema_and_metadata, [], non_selected_properties
            )

        # run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Sync With Connection 2
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2 = [catalog for catalog in found_catalogs_2
                           if catalog.get('stream_name') in streams_to_test]
        core_catalogs_2 = [catalog for catalog in test_catalogs_2 if not self.is_report(catalog['stream_name'])]
        # select all fields for core streams and...
        self.select_all_streams_and_fields(conn_id_2, core_catalogs_2, select_all_fields=True)
        # select 'default' fields for report streams
        for report in self.expected_default_fields().keys():
            if report not in streams_to_test:
                continue
            catalog = [catalog for catalog in test_catalogs_2
                       if catalog['stream_name'] == report][0]
            schema_and_metadata = menagerie.get_annotated_schema(conn_id_2, catalog['stream_id'])
            metadata = schema_and_metadata['metadata']
            properties = {md['breadcrumb'][-1]
                          for md in metadata
                          if len(md['breadcrumb']) > 0 and md['breadcrumb'][0] == 'properties'}
            expected_fields = self.expected_default_fields()[catalog['stream_name']]
            self.assertTrue(expected_fields.issubset(properties),
                            msg=f"{report} missing {expected_fields.difference(properties)}")
            non_selected_properties = properties.difference(expected_fields)
            connections.select_catalog_and_fields_via_metadata(
                conn_id_2, catalog, schema_and_metadata, [], non_selected_properties
            )

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                # TODO update this with the lookback window DOES IT APPLY TO START DATE?
                # expected_conversion_window = -1 * int(self.get_properties()['conversion_window'])
                expected_start_date_1 = self.timedelta_formatted(self.start_date_1, days=0) # expected_conversion_window)
                expected_start_date_2 = self.timedelta_formatted(self.start_date_2, days=0) # expected_conversion_window)

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)
                primary_keys_list_1 = [tuple(message['data'][expected_pk] for expected_pk in expected_primary_keys)
                                       for message in synced_records_1[stream]['messages']
                                       if message['action'] == 'upsert']
                primary_keys_list_2 = [tuple(message['data'][expected_pk] for expected_pk in expected_primary_keys)
                                       for message in synced_records_2[stream]['messages']
                                       if message['action'] == 'upsert']
                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                if self.is_report(stream):
                    # collect information specific to incremental streams from syncs 1 & 2
                    expected_replication_key = next(iter(self.expected_replication_keys().get(stream)))

                    replication_dates_1 = [row.get('data').get(expected_replication_key) for row in
                                           synced_records_1.get(stream, {'messages': []}).get('messages', [])
                                           if row.get('data')]
                    replication_dates_2 = [row.get('data').get(expected_replication_key) for row in
                                           synced_records_2.get(stream, {'messages': []}).get('messages', [])
                                           if row.get('data')]

                    # BUG_TDL-17827 | https://jira.talendforge.org/browse/TDL-17827
                    #                 Improperly formatted replication keys for report streams
                    print(f"DATE BOUNDARIES SYNC 1: {stream} {sorted(replication_dates_1)[0]} {sorted(replication_dates_1)[-1]}")
                    # Verify replication key is greater or equal to start_date for sync 1
                    expected_start_date = dt.strptime(expected_start_date_1, self.START_DATE_FORMAT)
                    for replication_date in replication_dates_1:
                        replication_date = dt.strptime(replication_date, self.REPLICATION_KEY_FORMAT)
                        self.assertGreaterEqual(replication_date, expected_start_date,
                                msg="Report pertains to a date prior to our start date.\n" +
                                "Sync start_date: {}\n".format(expected_start_date_1) +
                                "Record date: {} ".format(replication_date)
                        )

                    expected_start_date = dt.strptime(expected_start_date_2, self.START_DATE_FORMAT)
                    # Verify replication key is greater or equal to start_date for sync 2
                    for replication_date in replication_dates_2:
                        replication_date = dt.strptime(replication_date, self.REPLICATION_KEY_FORMAT)
                        self.assertGreaterEqual(replication_date, expected_start_date,
                                msg="Report pertains to a date prior to our start date.\n" +
                                "Sync start_date: {}\n".format(expected_start_date_2) +
                                "Record date: {} ".format(replication_date)
                        )

                    # Verify the number of records replicated in sync 1 is greater than the number
                    # of records replicated in sync 2
                    self.assertGreater(record_count_sync_1, record_count_sync_2)

                    # Verify the records replicated in sync 2 were also replicated in sync 1
                    self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1))

                else:

                    # Verify that the 2nd sync with a later start date (more recent) replicates
                    # the same number of records as the 1st sync.
                    self.assertEqual(record_count_sync_2, record_count_sync_1)

                    # Verify by primary key the same records are replicated in the 1st and 2nd syncs
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)
