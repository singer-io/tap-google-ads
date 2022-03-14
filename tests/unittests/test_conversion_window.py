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

class TestBookmarkWithinConversionWindow(unittest.TestCase):

    @patch('tap_google_ads.streams.make_request')
    def test_bookmark_within_default_conversion_window(self, fake_make_request):
        conversion_window = 30
        self.execute(conversion_window, fake_make_request)

    @patch('tap_google_ads.streams.make_request')
    def test_bookmark_within_60_day_conversion_window(self, fake_make_request):
        conversion_window = 60
        self.execute(conversion_window, fake_make_request)

    @patch('tap_google_ads.streams.make_request')
    def test_bookmark_within_90_day_conversion_window(self, fake_make_request):
        conversion_window = 90
        self.execute(conversion_window, fake_make_request)

    def execute(self, conversion_window, fake_make_request):
        start_date = datetime(2021, 12, 1, 0, 0, 0)

        # Create the stream so we can call sync
        my_report_stream = ReportStream(
            fields=[],
            google_ads_resource_names=['accessible_bidding_strategy'],
            resource_schema=resource_schema,
            primary_keys=['foo']
        )
        config = {
            "start_date": str(start_date),
            "conversion_window": str(conversion_window),
        }
        end_date = datetime.now()
        bookmark_value = str(end_date - timedelta(days=(conversion_window - 5)))

        state = {
            "currently_syncing": (None, None),
            "bookmarks": {"hi": {"123": {'date': bookmark_value}},}
        }
        my_report_stream.sync(
            Mock(),
            {"customerId": "123",
             "loginCustomerId": "456"},
            {"tap_stream_id": "hi",
             "stream": "hi",
             "metadata": []},
            config,
            state,
        )

        all_queries_requested = []
        for request_sent in fake_make_request.call_args_list:
            # The function signature is gas, query, customer_id
            _, query, _ = request_sent.args
            all_queries_requested.append(query)


        # Verify the first date queried is the conversion window date (not the bookmark)
        expected_first_query_date = str(end_date - timedelta(days=conversion_window))[:10]
        actual_first_query_date = str(all_queries_requested[0])[-11:-1]
        self.assertEqual(expected_first_query_date, actual_first_query_date)

        # Verify the number of days queried is based off the conversion window.
        self.assertEqual(len(all_queries_requested), conversion_window + 1) # inclusive

class TestBookmarkOnConversionWindow(unittest.TestCase):

    @patch('tap_google_ads.streams.make_request')
    def test_bookmark_on_1_day_conversion_window(self, fake_make_request):
        conversion_window = 1
        self.execute(conversion_window, fake_make_request)

    @patch('tap_google_ads.streams.make_request')
    def test_bookmark_on_default_conversion_window(self, fake_make_request):
        conversion_window = 30
        self.execute(conversion_window, fake_make_request)

    @patch('tap_google_ads.streams.make_request')
    def test_bookmark_on_60_day_conversion_window(self, fake_make_request):
        conversion_window = 60
        self.execute(conversion_window, fake_make_request)

    @patch('tap_google_ads.streams.make_request')
    def test_bookmark_on_90_day_conversion_window(self, fake_make_request):
        conversion_window = 90
        self.execute(conversion_window, fake_make_request)

    def execute(self, conversion_window, fake_make_request):
        start_date = datetime(2021, 12, 1, 0, 0, 0)

        # Create the stream so we can call sync
        my_report_stream = ReportStream(
            fields=[],
            google_ads_resource_names=['accessible_bidding_strategy'],
            resource_schema=resource_schema,
            primary_keys=['foo']
        )
        config = {
            "start_date": str(start_date),
            "conversion_window": str(conversion_window),
        }
        end_date = datetime.now()
        bookmark_value = str(end_date - timedelta(days=conversion_window))

        state = {
            "currently_syncing": (None, None),
            "bookmarks": {"hi": {"123": {'date': bookmark_value}},}
        }
        my_report_stream.sync(
            Mock(),
            {"customerId": "123",
             "loginCustomerId": "456"},
            {"tap_stream_id": "hi",
             "stream": "hi",
             "metadata": []},
            config,
            state,
        )

        all_queries_requested = []
        for request_sent in fake_make_request.call_args_list:
            # The function signature is gas, query, customer_id
            _, query, _ = request_sent.args
            all_queries_requested.append(query)


        # Verify the first date queried is the conversion window date (not the bookmark)
        expected_first_query_date = str(bookmark_value)[:10]
        actual_first_query_date = str(all_queries_requested[0])[-11:-1]
        self.assertEqual(expected_first_query_date, actual_first_query_date)

        # Verify the number of days queried is based off the conversion window.
        self.assertEqual(len(all_queries_requested), conversion_window + 1) # inclusive


if __name__ == '__main__':
    unittest.main()
