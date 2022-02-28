"""Test tap field exclusions via metadata."""
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

    def test_default_case(self):
        """
        Testing that field exclusion logic updates metadata as expected.
        Verify tap can perform sync for randomized combos of fields outside the `default` selection used by other feature tests.
        TODO add randomization.  Will require new method to select specific fields.
        """
        print("Field Exclusion Test for tap-google-ads report streams")

        # --- Test report streams throw an error --- #

        streams_to_test = {stream for stream in self.expected_streams()
                           if self.is_report(stream)}

        streams_to_test = streams_to_test - {
            # These streams missing from expected_default_fields() method
            'expanded_landing_page_report',
            'shopping_performance_report',
            'user_location_performance_report',
            'keywordless_query_report',
            'keywords_performance_report',
            'landing_page_report',

            # BUG | TODO No fieldExclusions on any stream
            # These streams have invalid segment / metric selections need more than default selection to replicate
            'click_performance_report',

            # TODO These streams have no data to replicate and fail the last assertion, keep in and skip assertion?
            'video_performance_report',
            'audience_performance_report',
            'placement_performance_report',
            'display_topics_performance_report',
            'display_keyword_performance_report',
        }
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

                # make second call to get field metadata
                schema = menagerie.get_annotated_schema(conn_id, catalogs_to_test[0]['stream_id'])
                field_exclusions = {
                    rec['breadcrumb'][1]: rec['metadata']['fieldExclusions']
                    for rec in schema['metadata']
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash"
                }

                print(f"Perform assertions for stream: {stream}")
                
                # verify selected and inclusion are as expected, none selected, all available at start
                for rec in schema['metadata']:
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash":
                        self.assertIn(rec['metadata']['inclusion'], ['available', 'automatic'],
                                        msg="Expected inclusion for field {} in ['available', 'automatic']".format(rec['breadcrumb'][1]))
                        self.assertEqual(rec['metadata']['selected'], False,
                                        msg="Expected selection for field {} = 'False'".format(rec['breadcrumb'][1]))

                # count fields with no exclusions and randomly add half to selection set
                no_exclusion_list = []
                for field, values in field_exclusions.items():
                    if values == []:
                        no_exclusion_list.append(field)
                #print("Count no_exclusion_list = {}".format(len(no_exclusion_list)))
                no_exclusion_field_set = random.sample(no_exclusion_list, k=int(len(no_exclusion_list)/2))
                #print(f"no_exclusion_field_set: {no_exclusion_field_set}")

                # count fields with exclusions and randomly pick 1 and add to selection set
                exclusion_list = []
                for field, values in field_exclusions.items():
                    if values != []:
                        exclusion_list.append(field)
                #print("Count exclusion_list = {}".format(len(exclusion_list)))
                if len(exclusion_list) == 0:
                    raise AssertionError(f"Skipping assertions. No field exclusions for stream: {stream}")
                else:
                   raise RuntimeError(f"This one is fine. stream: {stream}")
                
                add_exclusion_field = exclusion_list[random.randrange(len(exclusion_list))]
                #print(f"Field with exclusion(s) to add: {add_exclusion_field}")

                # select fields and re-pull annotated_schema. For now use default fields, add dynamic later
                self.select_all_streams_and_default_fields(conn_id, catalogs_to_test)

                # Collect updated metadata
                schema_2 = menagerie.get_annotated_schema(conn_id, catalogs_to_test[0]['stream_id'])
                field_exclusions_2 = {
                    rec['breadcrumb'][1]: rec['metadata']['fieldExclusions']
                    for rec in schema_2['metadata']
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash"
                }

                field_inclusions_2 = {
                    rec['breadcrumb'][1]: rec['metadata']['inclusion']
                    for rec in schema_2['metadata']
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash"
                }

                exclusion_list_2 = []
                for field, values in field_exclusions_2.items():
                    if values != []:
                        exclusion_list_2.append(field)
                #print("Count exclusion_list = {}".format(len(exclusion_list_2)))
                if len(exclusion_list_2) == 0:
                    raise AssertionError(f"Skipping assertions. No field exclusions for stream: {stream}")
                
                add_exclusion_field_2 = exclusion_list_2[random.randrange(len(exclusion_list_2))]
                #print(f"Field with exclusion(s) to add: {add_exclusion_field_2}")

                # verify selected and inclusion are as expected, none selected, all available
                # verify selected metadata for all fields, TODO available verification TBD
                for rec in schema_2['metadata']:
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash":
                        if rec['breadcrumb'][1] in self.expected_default_fields()[catalogs_to_test[0]['stream_name']]:

                            if rec['breadcrumb'][1] in exclusion_list_2:
                                # inclusion not available or automatic,  confirm removal pending dev info, field may not update
                                testVar = 1
                            else:
                                self.assertIn(rec['metadata']['inclusion'], ['available', 'automatic'],
                                                  msg="Expected inclusion for field {} in ['available', 'automatic']".format(rec['breadcrumb'][1]))
                            self.assertEqual(rec['metadata']['selected'], True,
                                        msg="Expected selection for field {} = 'True'".format(rec['breadcrumb'][1]))

                        else:  # Non default fields
                            if rec['breadcrumb'][1] in exclusion_list_2:  # Has exclusion fields
                                # inclusion not available or automatic, confirm removal pending dev info, field may not update
                                testVar = 1
                                
                            else:
                                self.assertIn(rec['metadata']['inclusion'], ['available', 'automatic'],
                                              msg="Expected inclusion for field {} in ['available', 'automatic']".format(rec['breadcrumb'][1]))
                                self.assertEqual(rec['metadata']['selected'], False,
                                        msg="Expected selection for field {} = 'False'".format(rec['breadcrumb'][1]))

                # update selected fields to add the next random available field with exclusions
                # select fields and re-pull annotated_schema?
                #   verify selected, and available for all fields
                # continue adding available fields with exclusions in random order until none left

                try:
                    # Run a sync
                    sync_job_name = self.run_and_verify_sync(conn_id)

                    # TODO additional assertions?

                finally:
                    # deselect stream once it's been tested
                    self.deselect_streams(conn_id, catalogs_to_test)
                

    def DONT_RUN_test_invalid_exclusion_selection(self):
        """
        Verify tap throws critical error with clear, concise error message when invalid field selection is performed for a report stream
        """
        print("Field Exclusions - Invalid selection test for tap-google-ads core streams")

        # --- Start testing report streams --- #

        streams_to_test = {stream for stream in self.expected_streams()
                           if self.is_report(stream)}

        #streams_to_test = {'placeholder_report'}  # Has exclusions
        #streams_to_test = {'click_performance_report'}  #  No exclusions

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
                    select_all_fields=True
                )

                # make second call to get field metadata
                schema = menagerie.get_annotated_schema(conn_id, catalogs_to_test[0]['stream_id'])
                field_exclusions = {
                    rec['breadcrumb'][1]: rec['metadata']['fieldExclusions']
                    for rec in schema['metadata']
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash"
                }

                # verify selected and inclusion are as expected, none selected, all available
                for rec in schema['metadata']:
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash":
                        self.assertIn(rec['metadata']['inclusion'], ['available', 'automatic'],
                                        msg="Expected inclusion for field {} in ['available', 'automatic']".format(rec['breadcrumb'][1]))
                        self.assertEqual(rec['metadata']['selected'], True,
                                        msg="Expected selection for field {} = 'True'".format(rec['breadcrumb'][1]))

                # count fields with no exclusions and randomly add half to selection set
                no_exclusion_list = []
                for field, values in field_exclusions.items():
                    if values == []:
                        no_exclusion_list.append(field)
                print("Count no_exclusion_list = {}".format(len(no_exclusion_list)))
                no_exclusion_field_set = random.sample(no_exclusion_list, k=int(len(no_exclusion_list)/2))
                print(f"no_exclusion_field_set: {no_exclusion_field_set}")

                # count fields with exclusions and randomly pick 1 and add to selection set
                exclusion_list = []
                for field, values in field_exclusions.items():
                    if values != []:
                        exclusion_list.append(field)
                print("Count exclusion_list = {}".format(len(exclusion_list)))
                if len(exclusion_list) == 0:
                    print(f"Skipping assertions. No field exclusions for stream: {stream}")
                    continue
                add_exclusion_field = exclusion_list[random.randrange(len(exclusion_list))]
                print(f"Field with exclusion(s) to add: {add_exclusion_field}")

                # Run a sync
                sync_job_name = runner.run_sync_mode(self, conn_id)
                
                exit_status = menagerie.get_exit_status(conn_id, sync_job_name)

                print(f"Perform assertions for stream: {stream}")
                self.assertEqual(1, exit_status.get('tap_exit_status'))
                self.assertEqual(0, exit_status.get('target_exit_status'))
                self.assertEqual(0, exit_status.get('discovery_exit_status'))
                self.assertIsNone(exit_status.get('check_exit_status'))
                
                # Verify error message tells user they must select an attribute/metric for the invalid stream
                self.assertIn(
                    "PROHIBITED_FIELD_COMBINATION_IN_SELECT_CLAUSE",
                    exit_status.get("tap_error_message")
                )
                self.assertIn(
                    "The following pairs of fields may not be selected together",
                    exit_status.get("tap_error_message")
                )

        # for stream in streams_to_test:
        #     with self.subTest(stream=stream):

        #         # # Verify that only the automatic fields are sent to the target.
        #         expected_auto_fields = self.expected_automatic_fields()
        #         expected_primary_key = list(self.expected_primary_keys()[stream])[0]  # assumes no compound-pks
        #         self.assertEqual(len(self.expected_primary_keys()[stream]), 1, msg="Compound pk not supported")
        #         for record in synced_records[stream]['messages']:

        #             record_primary_key_values = record['data'][expected_primary_key]
        #             record_keys = set(record['data'].keys())

        #             with self.subTest(primary_key=record_primary_key_values):
        #                 self.assertSetEqual(expected_auto_fields[stream], record_keys)

        #         # Verify that all replicated records have unique primary key values.
        #         actual_pks = [row.get('data').get(expected_primary_key) for row in
        #                       synced_records.get(stream, {'messages':[]}).get('messages', []) if row.get('data')]

        #         self.assertCountEqual(actual_pks, set(actual_pks))
