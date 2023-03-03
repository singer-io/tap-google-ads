import unittest
from datetime import datetime
from datetime import timedelta
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch
from tap_google_ads.streams import ReportStream
from tap_google_ads.streams import make_request
import singer
import pytz

resource_schema = {
    "accessible_bidding_strategy": {
        "fields": {}
    },

}

class TestEndDate(unittest.TestCase):

    def get_queries_from_sync(self, fake_make_request):
        all_queries_requested = []
        for request_sent in fake_make_request.call_args_list:
            # The function signature is gas, query, customer_id, config
            _, query, _, _ = request_sent.args
            all_queries_requested.append(query)
        return all_queries_requested

    def run_sync(self, start_date, end_date, fake_make_request):

        # Create the stream so we can call sync
        my_report_stream = ReportStream(
            fields=[],
            google_ads_resource_names=['accessible_bidding_strategy'],
            resource_schema=resource_schema,
            primary_keys=['foo']
        )

        # Create a config that maybe has an end date
        config = {"start_date": str(start_date),}

        # If end_date exists, write it to the config
        if end_date:
            config["end_date"] = str(end_date)

        my_report_stream.sync(
            Mock(),
            {"customerId": "123",
             "loginCustomerId": "456"},
            {"tap_stream_id": "hi",
             "stream": "hi",
             "metadata": []},
            config,
            {},
            None
        )

    @patch('singer.utils.now')
    @patch('tap_google_ads.streams.make_request')
    def test_no_end_date(self, fake_make_request, fake_datetime_now):
        start_date = datetime(2022, 1, 1, 0, 0, 0)
        end_date = datetime(2022, 3, 1, 0, 0, 0)

        # Adding tzinfo helped the mock to work and avoids a
        # TypeError(can't subtract offset-naive and offset-aware
        # datetimes) here in the test
        fake_datetime_now.return_value = end_date.replace(tzinfo=pytz.UTC)

        # Don't pass in end_date to test the tap's fallback to today
        self.run_sync(start_date, None, fake_make_request)
        all_queries_requested = self.get_queries_from_sync(fake_make_request)

        date_delta = end_date - start_date

        # Add one to make it inclusive of the end date
        days_between_start_and_end = date_delta.days + 1

        # Compute the range of expected days, because end_date will always shift
        expected_days = [
            '2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04',
            '2022-01-05', '2022-01-06', '2022-01-07', '2022-01-08',
            '2022-01-09', '2022-01-10', '2022-01-11', '2022-01-12',
            '2022-01-13', '2022-01-14', '2022-01-15', '2022-01-16',
            '2022-01-17', '2022-01-18', '2022-01-19', '2022-01-20',
            '2022-01-21', '2022-01-22', '2022-01-23', '2022-01-24',
            '2022-01-25', '2022-01-26', '2022-01-27', '2022-01-28',
            '2022-01-29', '2022-01-30', '2022-01-31', '2022-02-01',
            '2022-02-02', '2022-02-03', '2022-02-04', '2022-02-05',
            '2022-02-06', '2022-02-07', '2022-02-08', '2022-02-09',
            '2022-02-10', '2022-02-11', '2022-02-12', '2022-02-13',
            '2022-02-14', '2022-02-15', '2022-02-16', '2022-02-17',
            '2022-02-18', '2022-02-19', '2022-02-20', '2022-02-21',
            '2022-02-22', '2022-02-23', '2022-02-24', '2022-02-25',
            '2022-02-26', '2022-02-27', '2022-02-28', '2022-03-01',
        ]

        for day in expected_days:
            self.assertTrue(
                any(
                    day in query for query in all_queries_requested
                )
            )

    @patch('tap_google_ads.streams.make_request')
    def test_end_date_one_day_after_start(self, fake_make_request):
        start_date = datetime(2022, 3, 5, 0, 0, 0)
        end_date = datetime(2022, 3, 6, 0, 0, 0)
        self.run_sync(start_date, end_date, fake_make_request)
        all_queries_requested = self.get_queries_from_sync(fake_make_request)

        expected_days = [
            "2022-03-05",
            "2022-03-06",
        ]

        for day in expected_days:
            self.assertTrue(
                any(
                    day in query for query in all_queries_requested
                )
            )

    @patch('tap_google_ads.streams.make_request')
    def test_end_date_one_day_before_start(self, fake_make_request):
        start_date = datetime(2022, 3, 6, 0, 0, 0)
        end_date = datetime(2022, 3, 5, 0, 0, 0)
        self.run_sync(start_date, end_date, fake_make_request)
        all_queries_requested = self.get_queries_from_sync(fake_make_request)

        # verify no requests are made with an invalid start/end date configuration
        self.assertEqual(len(all_queries_requested), 0)


if __name__ == '__main__':
    unittest.main()
