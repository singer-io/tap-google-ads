"""Test tap field exclusions for invalid selection sets."""
import re
import random

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class FieldExclusionInvalidGoogleAds(GoogleAdsBase):
    """
    Test tap's field exclusion logic with invalid selection for all streams
    TODO Verify when given field selected, `fieldExclusions` fields in metadata are grayed out and cannot be selected (Manual case)
    """

    @staticmethod
    def name():
        return "tt_google_ads_field_exclusion_invalid"

    def random_field_gather(self, input_fields_with_exclusions):
        """
        Method takes list of fields with exclusions and generates a random set fields without conflicts as a result
        The set of fields with exclusions is generated in random order so that different combinations of fields can
        be tested over time.  A single invalid field is then added to violate exclusion rules.
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

        # Now add one more exclusion field to make the selection invalid
        # Select a field from our list and random
        invalid_field_partner = randomly_selected_list_of_fields_with_exclusions[
            random.randrange(len(randomly_selected_list_of_fields_with_exclusions))]
        # Select a field from that fields exclusion list
        invalid_field = self.field_exclusions[invalid_field_partner][
            random.randrange(len(self.field_exclusions[invalid_field_partner]))]
        # Add the invalid field to the lists
        self.random_order_of_exclusion_fields[self.stream].append(invalid_field,)
        randomly_selected_list_of_fields_with_exclusions.append(invalid_field)

        exclusion_fields_to_select = randomly_selected_list_of_fields_with_exclusions
        #print(f"random_order_of_exclusion_fields: {self.random_order_of_exclusion_fields}")

        return exclusion_fields_to_select


    def test_invalid_case(self):
        """
        Verify tap generates suitable error message when randomized combination of fields voilates exclusion rules.

        Established randomization for valid field selection using new method to select specific fields.
        Implemented random selection in valid selection test, then added a new field randomly to violate exclusion rules.

        """
        print("Field Exclusions - Invalid selection test for tap-google-ads report streams")

        # --- Test report streams --- #

        streams_to_test = {stream for stream in self.expected_streams()
                           if self.is_report(stream)} - {'click_performance_report'}  #  No exclusions

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

                # make second call to get field metadata
                schema = menagerie.get_annotated_schema(conn_id, catalogs_to_test[0]['stream_id'])
                field_exclusions = {
                    rec['breadcrumb'][1]: rec['metadata']['fieldExclusions']
                    for rec in schema['metadata']
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash"
                }

                self.field_exclusions = field_exclusions

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
                        sync_job_name = runner.run_sync_mode(self, conn_id)
                        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)

                        print(f"Perform assertions for stream: {stream}")
                        self.assertEqual(1, exit_status.get('tap_exit_status'))
                        self.assertEqual(0, exit_status.get('target_exit_status'))
                        self.assertEqual(0, exit_status.get('discovery_exit_status'))
                        self.assertIsNone(exit_status.get('check_exit_status'))

                        # Verify error message tells user they must select an attribute/metric for the invalid stream
                        # TODO build list of strings to test in future

                        # Initial assertion group generated if all fields selelcted
                        # self.assertIn(
                        #     "PROHIBITED_FIELD_COMBINATION_IN_SELECT_CLAUSE",
                        #     exit_status.get("tap_error_message")
                        # )
                        # self.assertIn(
                        #     "The following pairs of fields may not be selected together",
                        #     exit_status.get("tap_error_message")
                        # )

                        # New error message if random selection method is used
                        # PROHIBITED_SEGMENT_WITH_METRIC_IN_SELECT_OR_WHERE_CLAUSE

                        # TODO additional assertions?

                    finally:
                        # deselect stream once it's been tested
                        self.deselect_streams(conn_id, catalogs_to_test)
