"""Test tap field exclusions with random field selection."""
import re
import random

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class FieldExclusionGoogleAds(GoogleAdsBase):
    """
    Test tap's field exclusion logic for all streams
    TODO Verify when given field selected, `fieldExclusions` fields in metadata are grayed out and cannot be selected (Manual case)
    """

    @staticmethod
    def name():
        return "tt_google_ads_field_exclusion"

    def random_field_gather(self, input_fields_with_exclusions):
        """
        Method takes list of fields with exclusions and generates a random set fields without conflicts as a result
        The set of fields with exclusions is generated in random order so that different combinations of fields can
        be tested over time.
        """

        # Build random set of fields with exclusions.  Select as many as possible
        randomly_selected_list_of_fields_with_exclusions = []
        remaining_available_fields_with_exclusions = input_fields_with_exclusions
        #print(f"Starting with {len(remaining_available_fields_with_exclusions)} exclusion fields")
        while len(remaining_available_fields_with_exclusions) > 0:
            # randomly select one field that has exclusions
            newly_added_field = remaining_available_fields_with_exclusions[
                random.randrange(len(remaining_available_fields_with_exclusions))]
            # save list for debug incase test fails
            self.random_order_of_exclusion_fields[self.stream].append(newly_added_field,)
            randomly_selected_list_of_fields_with_exclusions.append(newly_added_field)
            # Update remaining_available_fields_with_exclusinos based on random selection
            newly_excluded_fields_to_remove = self.field_exclusions[newly_added_field]
            # Remove newly selected field
            remaining_available_fields_with_exclusions.remove(newly_added_field)
            # Remove associated excluded fields
            for field in newly_excluded_fields_to_remove:
                if field in remaining_available_fields_with_exclusions:
                    remaining_available_fields_with_exclusions.remove(field)

        exclusion_fields_to_select = randomly_selected_list_of_fields_with_exclusions
        #print(f"random_order_of_exclusion_fields: {self.random_order_of_exclusion_fields}")

        return exclusion_fields_to_select


    def test_default_case(self):
        """
        Verify tap can perform sync for random combinations of fields that do not violate exclusion rules.
        Established randomization for valid field selection using new method to select specific fields.
        """
        print("Field Exclusion Test with random field selection for tap-google-ads report streams")

        # --- Test report streams --- #

        streams_to_test = {stream for stream in self.expected_streams()
                           if self.is_report(stream)} - {'click_performance_report'}  #  No exclusions

        streams_to_test = streams_to_test - {
            # These streams missing from expected_default_fields() method TODO unblocked due to random? Test them now
            # 'expanded_landing_page_report',
            # 'shopping_performance_report',
            # 'user_location_performance_report',
            # 'keywordless_query_report',
            # 'keywords_performance_report',
            # 'landing_page_report',
            # TODO These streams have no data to replicate and fail the last assertion
            'video_performance_report',
            'audience_performance_report',
            'placement_performance_report',
            'display_topics_performance_report',
            'display_keyword_performance_report',
        }

        #streams_to_test = {'gender_performance_report', 'placeholder_report',}

        random_order_of_exclusion_fields = {}
        conn_id = connections.ensure_connection(self)

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                catalogs_to_test = [catalog
                                    for catalog in found_catalogs
                                    if catalog["stream_name"] == stream]

                # select all fields for core streams and...
                self.select_all_streams_and_fields(
                    conn_id,
                    catalogs_to_test,
                    select_all_fields=False
                )

                # make second call to get field level metadata
                schema = menagerie.get_annotated_schema(conn_id, catalogs_to_test[0]['stream_id'])
                field_exclusions = {
                    rec['breadcrumb'][1]: rec['metadata']['fieldExclusions']
                    for rec in schema['metadata']
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash"
                }

                self.field_exclusions = field_exclusions  # expose filed_exclusions globally

                print(f"Perform assertions for stream: {stream}")

                # Verify all fields start with 'selected' = False
                for rec in schema['metadata']:
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash":
                        self.assertEqual(rec['metadata']['selected'], False,
                                        msg="Expected selection for field {} = 'False'".format(rec['breadcrumb'][1]))

                # gather fields with no exclusions so they can all be added to selection set
                fields_without_exclusions = []
                for field, values in field_exclusions.items():
                    if values == []:
                        fields_without_exclusions.append(field)

                # gather fields with exclusions as input to randomly build maximum length selection set
                fields_with_exclusions = []
                for field, values in field_exclusions.items():
                    if values != []:
                       fields_with_exclusions.append(field)
                #print("Count exclusion_list = {}".format(len(fields_with_exclusions)))
                if len(fields_with_exclusions) == 0:
                    raise AssertionError(f"Skipping assertions. No field exclusions for stream: {stream}")

                self.stream = stream
                random_order_of_exclusion_fields[stream] = []
                self.random_order_of_exclusion_fields = random_order_of_exclusion_fields

                random_exclusion_field_selection_list = self.random_field_gather(fields_with_exclusions)
                field_selection_set = set(random_exclusion_field_selection_list + fields_without_exclusions)

                with self.subTest(order_of_fields_selected=self.random_order_of_exclusion_fields[stream]):

                    # select fields and re-pull annotated_schema.
                    self.select_stream_and_specified_fields(conn_id, catalogs_to_test[0], field_selection_set)

                    try:
                        # Collect updated metadata
                        schema_2 = menagerie.get_annotated_schema(conn_id, catalogs_to_test[0]['stream_id'])

                        # Verify metadata for all fields
                        for rec in schema_2['metadata']:
                            if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash":
                                # Verify metadata for selected fields
                                if rec['breadcrumb'][1] in field_selection_set:
                                    self.assertEqual(rec['metadata']['selected'], True,
                                                msg="Expected selection for field {} = 'True'".format(rec['breadcrumb'][1]))

                                else:  # Verify metadata for non selected fields
                                    self.assertEqual(rec['metadata']['selected'], False,
                                                     msg="Expected selection for field {} = 'False'".format(rec['breadcrumb'][1]))

                        # Run a sync
                        #sync_job_name = self.run_and_verify_sync(conn_id)
                        sync_job_name = runner.run_sync_mode(self, conn_id)
                        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)

                        self.assertEqual(0, exit_status.get('tap_exit_status'))
                        self.assertEqual(0, exit_status.get('target_exit_status'))
                        self.assertEqual(0, exit_status.get('discovery_exit_status'))
                        self.assertIsNone(exit_status.get('check_exit_status'))

                        # These streams likely replicate records using the default field selection but may not produce any
                        # records when selecting this many fields with exclusions.
                        streams_unlikely_to_replicate_records = {
                            'ad_performance_report',
                            'account_performance_report',
                            'shopping_performance_report',
                            'search_query_performance_report',
                            'placeholder_feed_item_report',
                            'placeholder_report',
                            'keywords_performance_report',
                            'keywordless_query_report',
                            'geo_performance_report',
                            'gender_performance_report',  # Very rare
                            'ad_group_audience_performance_report',
                            'age_range_performance_report',
                            'campaign_audience_performance_report',
                            'user_location_performance_report',
                            'ad_group_performance_report',
                            }

                        if stream not in streams_unlikely_to_replicate_records:
                            sync_record_count = runner.examine_target_output_file(
                                self, conn_id, self.expected_streams(), self.expected_primary_keys())
                            self.assertGreater(
                                sum(sync_record_count.values()), 0,
                                msg="failed to replicate any data: {}".format(sync_record_count)
                            )
                            print("total replicated row count: {}".format(sum(sync_record_count.values())))

                        # TODO additional assertions?

                    finally:
                        # deselect stream once it's been tested
                        self.deselect_streams(conn_id, catalogs_to_test)
