from tap_tester import menagerie, connections, runner
from math import ceil
from base import GoogleAdsBase

class TesGoogleAdstPagination(GoogleAdsBase):
    """
    Ensure tap can replicate multiple pages of data for streams that use query limit.
    """
    DEFAULT_QUERY_LIMIT = 2

    @staticmethod
    def name():
        return "tt_google_ads_pagination"

    def test_run(self):
        """
        • Verify that for each stream you can get multiple pages of data.  
        This requires we ensure more than 1 page of data exists at all times for any given stream.
        • Verify by pks that the data replicated matches the data we expect.
        """
        streams_to_test = {stream for stream in self.expected_streams()
                          if not self.is_report(stream)}

        # LIMIT parameter is not availble for call_details, campaign_labels, campaign_criterion, ad_group_criterion
        streams_to_test = streams_to_test - {'ad_group_criterion', 'call_details', 'campaign_labels', 'campaign_criterion'}
        
        # We do not have enough records for accessible_bidding_strategies, accounts, bidding_strategies, and user_list streams.
        streams_to_test = streams_to_test - {'accessible_bidding_strategies', 'accounts', 'bidding_strategies', 'user_list'}

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
                self.assertGreater(record_count_sync, self.DEFAULT_QUERY_LIMIT,
                                    msg="The number of records is not over the stream max limit")

                primary_keys_list = [tuple([message.get('data').get(expected_pk) for expected_pk in expected_primary_keys])
                                    for message in synced_records.get(stream).get('messages')
                                    if message.get('action') == 'upsert']

                # Chunk the replicated records (just primary keys) into expected pages
                pages = []
                page_count = ceil(len(primary_keys_list) / self.DEFAULT_QUERY_LIMIT)
                query_limit = self.DEFAULT_QUERY_LIMIT
                for page_index in range(page_count):
                    page_start = page_index * query_limit
                    page_end = (page_index + 1) * query_limit
                    pages.append(set(primary_keys_list[page_start:page_end]))

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):

                        for other_index, other_page in enumerate(pages):
                            if current_index == other_index:
                                continue  # don't compare the page to itself

                            self.assertTrue(
                                current_page.isdisjoint(other_page), msg=f'other_page_primary_keys={other_page}'
                            )
