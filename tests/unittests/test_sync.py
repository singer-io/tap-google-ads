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

    def run_test(self, start_date, end_date, fake_make_request):

        # Create the stream so we can call sync
        my_report_stream = ReportStream(
            fields=[],
            google_ads_resource_names=['accessible_bidding_strategy'],
            resource_schema=resource_schema,
            primary_keys=['foo']
        )

        # Create a config that maybe has an end date
        config = {"start_date": str(start_date),}

        if end_date:
            config["end_date"] = str(end_date)
        else:
            end_date = datetime.now()

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

        all_queries_requested = []
        for request_sent in fake_make_request.call_args_list:
            # The function signature is gas, query, customer_id
            _, query, _ = request_sent.args
            all_queries_requested.append(query)

        delta_days = end_date - start_date
        expected_days = [start_date]
        for i in range(delta_days.days):
            expected_days.append(
                start_date + timedelta(days=1)
            )


        for day in expected_days:
            # YYYY-MM-DD
            date_portion = str(day)[:10]

            self.assertTrue(
                any(
                    date_portion in query for query in all_queries_requested
                )
            )


    @patch('tap_google_ads.streams.make_request')
    def test_no_end_date(self, fake_make_request):
        start_date = datetime(2022, 1, 1, 0, 0, 0)
        end_date = None
        self.run_test(start_date, end_date, fake_make_request)

    @patch('tap_google_ads.streams.make_request')
    def test_end_date_one_day_after_start(self, fake_make_request):
        start_date = datetime(2022, 3, 5, 0, 0, 0)
        end_date = datetime(2022, 3, 6, 0, 0, 0)
        self.run_test(start_date, end_date, fake_make_request)

if __name__ == '__main__':
    unittest.main()
