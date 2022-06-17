from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase

class TesGoogleAdstPagination(GoogleAdsBase):
    """
    Ensure tap can replicate multiple pages of data for streams that use page limit.
    """
    API_LIMIT = 1

    @staticmethod
    def name():
        return "tt_google_ads_pagination"

    def test_run(self):
        """
        • Verify that for each stream you can get multiple pages of data.  
        This requires we ensure more than 1 page of data exists at all times for any given stream.
        • Verify by pks that the data replicated matches the data we expect.
        """
        streams_to_test = {'ad_groups', 'ads', 'campaign_budgets', 'campaigns', 'labels', 'carrier_constant', 
                           'feed', 'feed_item', 'language_constant', 'mobile_app_category_constant',
                           'mobile_device_constant', 'operating_system_version_constant', 'topic_constant',
                           'user_interest'}

        # Create connection
        conn_id = connections.ensure_connection(self)

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Partition catalogs for use in table/field selection
        test_catalogs = [catalog for catalog in found_catalogs
                           if catalog.get('stream_name') in streams_to_test]

        # Select fields
        self.select_all_streams_and_fields(conn_id, test_catalogs, select_all_fields=False)

        # Run a sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)

        # Acquire records from target output
        synced_records = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
         
                # Verify that we can paginate with all fields selected
                record_count_sync = record_count_by_stream.get(stream, 0)
                self.assertGreater(record_count_sync, self.LIMIT,
                                    msg="The number of records is not over the stream max limit")

                primary_keys_list = [tuple([message.get('data').get(expected_pk) for expected_pk in expected_primary_keys])
                                    for message in synced_records.get(stream).get('messages')
                                    if message.get('action') == 'upsert']

                primary_keys_list_1 = primary_keys_list[:self.LIMIT]
                primary_keys_list_2 = primary_keys_list[self.LIMIT:2*self.LIMIT]

                primary_keys_page_1 = set(primary_keys_list_1)
                primary_keys_page_2 = set(primary_keys_list_2)

                # Verify by primary keys that data is unique for page
                self.assertTrue(
                    primary_keys_page_1.isdisjoint(primary_keys_page_2))