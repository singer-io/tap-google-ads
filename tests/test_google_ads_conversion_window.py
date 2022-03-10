"""Test tap configurable properties. Specifically the conversion_window"""
import os
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class ConversionWindowBaseTest(GoogleAdsBase):
    """
    Test tap's sync mode can execute with valid conversion_window values set.

    Validate setting the conversion_window configurable property.

    Test Cases:

    Verify tap throws critical error when a value is provided directly by a user which is
    outside the set of acceptable values.

    Verify connection can be created, and tap can discover and sync with a conversion window
    set to the following values
      Acceptable values: { 1 through 30, 60, 90}
    """
    conversion_window = ''

    def name(self):
        return f"tt_google_ads_conv_window_{self.conversion_window}"

    def get_properties(self):
        """Configurable properties, with a switch to override the 'start_date' property"""
        return_value = {
            'start_date':dt.strftime(dt.utcnow() - timedelta(days=91), self.START_DATE_FORMAT),
            'user_id':      'not used?', # TODO ?
            'customer_ids': '5548074409,2728292456',
            'conversion_window': self.conversion_window,
            'login_customer_ids': [{"customerId": "5548074409", "loginCustomerId": "2728292456",}],
        }
        return return_value

    def run_test(self):
        """
        Testing that basic sync functions without Critical Errors when
        a valid conversion_windown is set.
        """
        print("Configurable Properties Test (conversion_window)")

        conn_id = connections.ensure_connection(self)

        streams_to_test = self.expected_streams() - {
            # TODO_TDL-17885 the following are not yet implemented
            'display_keyword_performance_report', # no test data available
            'display_topics_performance_report',  # no test data available
            'audience_performance_report',  # Potential BUG see above
            'placement_performance_report',  # no test data available
            "keywords_performance_report",  # no test data available
            "keywordless_query_report",  # no test data available
            "shopping_performance_report",  # cannot find this in GoogleUI
            "video_performance_report",  # no test data available
            "user_location_performance_report",  # no test data available
            'landing_page_report',  # not attempted 
            'expanded_landing_page_report', # not attempted 
        }
        streams_to_test = {'account_performance_report'}

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Perform table and field selection...
        core_catalogs = [catalog for catalog in found_catalogs
                         if not self.is_report(catalog['stream_name'])
                         and catalog['stream_name'] in streams_to_test]
        report_catalogs = [catalog for catalog in found_catalogs
                           if self.is_report(catalog['stream_name'])
                           and catalog['stream_name'] in streams_to_test]
        # select all fields for core streams and...
        self.select_all_streams_and_fields(conn_id, core_catalogs, select_all_fields=True)
        # select 'default' fields for report streams
        self.select_all_streams_and_default_fields(conn_id, report_catalogs)

        # set state to ensure conversion window is used
        today_datetime = dt.strftime(dt.utcnow(), self.REPLICATION_KEY_FORMAT)
        # today_datetime = dt.strftime(dt.utcnow(), self.REPLICATION_KEY_FORMAT)
        customer_id = os.getenv('TAP_GOOGLE_ADS_CUSTOMER_ID')
        initial_state = {
            'currently_syncing': [None, None],
            'bookmarks': {stream: {customer_id: {'date': today_datetime}}
                          for stream in streams_to_test
                          if self.is_report(stream)}
        }
        menagerie.set_state(conn_id, initial_state)

        # Run a sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify tap replicates through today by check state
        final_state = menagerie.get_state(conn_id)
        self.assertDictEqual(final_state, initial_state)

class ConversionWindowTestOne(ConversionWindowBaseTest):

    conversion_window = '1'

    def test_run(self):
        self.run_test()

# class ConversionWindowTestThirty(ConversionWindowBaseTest):

#     conversion_window = '30'

#     def test_run(self):
#         self.run_test()

# class ConversionWindowTestSixty(ConversionWindowBaseTest):

#     conversion_window = '60'

#     def test_run(self):
#         self.run_test()

# class ConversionWindowTestNinety(ConversionWindowBaseTest):

#     conversion_window = '90'

#     def test_run(self):
#         self.run_test()
