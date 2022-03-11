import unittest
from datetime import datetime
from datetime import timedelta
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch
from tap_google_ads.streams import ReportStream
from tap_google_ads.streams import make_request


resource_schema = {
    "accessible_bidding_strategy": {
        "fields": {}
    },

}

class TestEndDate(unittest.TestCase):

    def get_queries_from_sync(self, fake_make_request):
        all_queries_requested = []
        for request_sent in fake_make_request.call_args_list:
            # The function signature is gas, query, customer_id
            _, query, _ = request_sent.args
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
            {}
        )

    @patch('tap_google_ads.streams.make_request')
    def test_no_end_date(self, fake_make_request):
        start_date = datetime(2022, 1, 1, 0, 0, 0)
        end_date = datetime.now()
        # Don't pass in end_date to test the tap's fallback to today
        self.run_sync(start_date, None, fake_make_request)
        all_queries_requested = self.get_queries_from_sync(fake_make_request)

        date_delta = end_date - start_date

        # Add one to make it inclusive of the end date
        days_between_start_and_end = date_delta.days + 1

        # Compute the range of expected days, because end_date will always shift
        expected_days = []
        for i in range(days_between_start_and_end):
            expected_datetime = start_date + timedelta(days=i)
            expected_day = str(expected_datetime)[:10]
            expected_days.append(expected_day)

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

if __name__ == '__main__':
    unittest.main()
