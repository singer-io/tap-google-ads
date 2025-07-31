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
        return {
            'start_date': dt.strftime(dt.utcnow() - timedelta(days=91), self.START_DATE_FORMAT),
            'user_id': 'not used?',
            'customer_ids': ','.join(self.get_customer_ids()),
            'conversion_window': self.conversion_window,
            'login_customer_ids': [{"customerId": os.getenv('TAP_GOOGLE_ADS_CUSTOMER_ID'),
                                    "loginCustomerId": os.getenv('TAP_GOOGLE_ADS_LOGIN_CUSTOMER_ID'),}],
        }

    def run_test(self):
        """
        Testing that basic sync functions without Critical Errors when
        a valid conversion_window is set.
        """
        print("Configurable Properties Test (conversion_window)")

        streams_to_test = {
            'campaigns',
            'account_performance_report',
            'assets'
        }

        # Create a connection
        conn_id = connections.ensure_connection(self)

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
        customer_id = os.getenv('TAP_GOOGLE_ADS_CUSTOMER_ID')
        initial_state = {
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
        final_state.pop('last_exception_triggered', None)
        self.assertDictEqual(final_state, initial_state)


class ConversionWindowTestOne(ConversionWindowBaseTest):

    conversion_window = '1'

    def test_run(self):
        self.run_test()

class ConversionWindowTestThirty(ConversionWindowBaseTest):

    conversion_window = '30'

    def test_run(self):
        self.run_test()

class ConversionWindowTestSixty(ConversionWindowBaseTest):

    conversion_window = '60'

    def test_run(self):
        self.run_test()

class ConversionWindowTestNinety(ConversionWindowBaseTest):

    conversion_window = '90'

    def test_run(self):
        self.run_test()
