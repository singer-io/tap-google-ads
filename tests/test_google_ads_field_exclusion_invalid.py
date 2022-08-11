"""Test tap field exclusions for invalid selection sets."""
import random
import pprint
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class FieldExclusionInvalidGoogleAds(GoogleAdsBase):
    """
    Test tap's field exclusion logic with invalid selection for all streams

    NOTE: Manual test case must be run at least once any time this feature changes or is updated.
          Verify when given field selected, `fieldExclusions` fields in metadata are grayed out and cannot be selected (Manually)
    """

    @staticmethod
    def name():
        return "tt_google_ads_field_exclusion_invalid"

    def choose_randomly(self, collection):
        return random.choice(list(collection))

    def random_field_gather(self):
        """
        Method takes list of fields with exclusions and generates a random set fields without conflicts as a result
        The set of fields with exclusions is generated in random order so that different combinations of fields can
        be tested over time.  A single invalid field is then added to violate exclusion rules.
        """

        random_selection = []

        # Assemble a valid selection of fields with exclusions
        remaining_fields = list(self.fields_with_exclusions)
        while remaining_fields:

            # Choose randomly from the remaining fields
            field_to_select = self.choose_randomly(remaining_fields)
            random_selection.append(field_to_select)

            # Remove field and it's excluded fields from remaining
            remaining_fields.remove(field_to_select)
            for field in self.field_exclusions[field_to_select]:
                if field in remaining_fields:
                    remaining_fields.remove(field)

             # Save list for debug incase test fails
            self.random_order_of_exclusion_fields[self.stream].append(field_to_select)

        # Now add one more exclusion field to make the selection invalid
        while True:
            # Choose randomly from the selected fields
            random_field = self.choose_randomly(random_selection)

            # Choose randomly from that field's supported excluded fields
            excluded_fields = set(self.field_exclusions[random_field])
            supported_excluded_fields = {field for field in excluded_fields
                                     if field in self.fields_with_exclusions}
            if supported_excluded_fields:
                invalid_field = self.choose_randomly(supported_excluded_fields)
                break

        # Add this invalid field to the selection
        random_selection.append(invalid_field)
        self.random_order_of_exclusion_fields[self.stream].append(invalid_field,)

        return random_selection


    def test_invalid_case(self):
        """
        Verify tap generates suitable error message when randomized combination of fields voilate exclusion rules.

        Established randomization for valid field selection using new method to select specific fields.
        Implemented random selection in valid selection test, then added a new field randomly to violate exclusion rules.

        """
        print("Field Exclusions - Invalid selection test for tap-google-ads report streams")

        # --- Test report streams --- #

        streams_to_test = {stream for stream in self.expected_streams()
                           if self.is_report(stream)} - {'click_performance_report'}  # No exclusions, skipped intentionally

        # streams_to_test = {'search_query_performance_report'} # , 'placeholder_report',}

        random_order_of_exclusion_fields = {}
        tap_exit_status_by_stream = {}
        exclusion_errors = {}

        # bump start date from default
        self.start_date = dt.strftime(dt.today() - timedelta(days=1), self.START_DATE_FORMAT)
        self.end_date = None
        conn_id = connections.ensure_connection(self, original_properties=False)

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                catalogs_to_test = [catalog
                                    for catalog in found_catalogs
                                    if catalog["stream_name"] == stream]

                # Make second call to get field metadata
                schema = menagerie.get_annotated_schema(conn_id, catalogs_to_test[0]['stream_id'])
                self.field_exclusions = {
                    rec['breadcrumb'][1]: rec['metadata']['fieldExclusions']
                    for rec in schema['metadata']
                    if rec['breadcrumb'] != [] and rec['breadcrumb'][1] != "_sdc_record_hash"
                }

                # Gather fields with no exclusions so they can all be added to selection set
                self.fields_without_exclusions = []
                for field, values in self.field_exclusions.items():
                    if values == []:
                        self.fields_without_exclusions.append(field)

                # Gather fields with exclusions as input to randomly build maximum length selection set
                self.fields_with_exclusions = []
                for field, values in self.field_exclusions.items():
                    if values != []:
                       self.fields_with_exclusions.append(field)
                if len(self.fields_with_exclusions) == 0:
                    raise AssertionError(f"Skipping assertions. No field exclusions for stream: {stream}")

                # Add new key to existing dicts
                random_order_of_exclusion_fields[stream] = []

                # Expose variables globally
                self.stream = stream
                self.random_order_of_exclusion_fields = random_order_of_exclusion_fields

                # Build random lists
                random_exclusion_field_selection_list = self.random_field_gather()
                field_selection_set = set(random_exclusion_field_selection_list + self.fields_without_exclusions)

                with self.subTest(order_of_fields_selected=self.random_order_of_exclusion_fields[stream]):

                    # Select fields and re-pull annotated_schema.
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
                        if exit_status.get('target_exit_status') == 1:
                            print(f"Stream {stream} has tap_exit_status = {exit_status.get('tap_exit_status')}\n" +
                                  "Message: {exit_status.get('tap_error_message')")
                            tap_exit_status_by_stream[stream] = exit_status.get('tap_exit_status')
                        else:
                            print(f"\n*** {stream} tap_exit_status {exit_status.get('tap_exit_status')} ***\n")
                            tap_exit_status_by_stream[stream] = exit_status.get('tap_exit_status')
                        self.assertEqual(1, exit_status.get('tap_exit_status'))
                        self.assertEqual(0, exit_status.get('target_exit_status'))
                        self.assertEqual(0, exit_status.get('discovery_exit_status'))
                        self.assertIsNone(exit_status.get('check_exit_status'))

                        # Verify error message tells user they must select an attribute/metric for the invalid stream
                        error_messages = ["The following pairs of fields may not be selected together",
                                          "Cannot select or filter on the following",
                                          "Cannot select the following",]
                        self.assertTrue(
                            any([error_message in exit_status.get("tap_error_message")
                                 for error_message in error_messages]),
                            msg=f'Unexpected Error Message: {exit_status.get("tap_error_message")}')
                        print(f"\n*** {stream} tap_error_message {exit_status.get('tap_error_message')} ***\n")
                    finally:
                        # deselect stream once it's been tested
                        self.deselect_streams(conn_id, catalogs_to_test)

        print("Streams tested: {}\ntap_exit_status_by_stream: {}".format(len(streams_to_test), tap_exit_status_by_stream))
