"""Test tap configurable properties. Specifically the conversion_window"""
import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class ConversionWindowInvalidTest(GoogleAdsBase):
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
        return f"tt_googleads_conversion_invalid_{self.conversion_window}"

    def get_properties(self):
        """Configurable properties, with a switch to override the 'start_date' property"""
        return {
            'start_date': dt.strftime(dt.utcnow() - timedelta(days=91), self.START_DATE_FORMAT),
            'user_id': 'not used?', # TODO ?
            'customer_ids': ','.join(self.get_customer_ids()),
            'conversion_window': self.conversion_window,
            'login_customer_ids': [{"customerId": os.getenv('TAP_GOOGLE_ADS_CUSTOMER_ID'),
                                    "loginCustomerId": os.getenv('TAP_GOOGLE_ADS_LOGIN_CUSTOMER_ID'),}],
        }

    def run_test(self):
        """
        Testing that basic sync functions without Critical Errors when
        a valid conversion_windown is set.
        """
        print("Configurable Properties Test (conversion_window)")

        streams_to_test = {
            'campagins',
            'account_performance_report',
        }

        try:
            # Create a connection
            conn_id = connections.ensure_connection(self)

            with self.subTest():
                raise AssertionError(f"Conenction should not have been created with conversion_window: "
                                     f"value {self.conversion_window}, type {type(self.conversion_window)}")

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

            # Verify the tap and target throw a critical error
            exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
            menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

            # Verify tap replicates through today by check state
            final_state = menagerie.get_state(conn_id)
            self.assertDictEqual(final_state, initial_state)

            with self.subTest():
                raise AssertionError("Tap should not have ran sync with conversion_window: "
                                     f"value {self.conversion_window}, type {type(self.conversion_window)}")

        except Exception as ex:
            err_msg_1 = "'message': 'properties do not match schema'"
            err_msg_2 = "'bad_properties': ['conversion_window']"

            print("Expected exception occurred.")
            
            # Verify connection cannot be made with invalid conversion_window
            print(f"Validating error message contains {err_msg_1}")
            self.assertIn(err_msg_1, ex.args[0])
            print(f"Validating error message contains {err_msg_2}")
            self.assertIn(err_msg_2, ex.args[0])


class ConversionWindowTestZeroInteger(ConversionWindowInvalidTest):

    conversion_window = 0

    def test_run(self):
        self.run_test()


class ConversionWindowTestZeroString(ConversionWindowInvalidTest):

    conversion_window = '0'

    def test_run(self):
        self.run_test()
