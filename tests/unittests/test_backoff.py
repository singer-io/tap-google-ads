import unittest
from unittest.mock import Mock, patch
from tap_google_ads.streams import make_request
from google.api_core.exceptions import InternalServerError, BadGateway, MethodNotImplemented, ServiceUnavailable, GatewayTimeout, TooManyRequests
from requests.exceptions import ReadTimeout

@patch('time.sleep')
class TestBackoff(unittest.TestCase):

    def test_500_internal_server_error(self, mock_sleep):
        """
        Check whether the tap backoffs properly for 5 times in case of InternalServerError.
        """
        mocked_google_ads_client = Mock()
        mocked_google_ads_client.search.side_effect = InternalServerError("Internal error encountered")

        try:
            make_request(mocked_google_ads_client, "", "")
        except InternalServerError:
            pass

        # Verify that tap backoff for 5 times
        self.assertEqual(mocked_google_ads_client.search.call_count, 5)

    def test_501_not_implemented_error(self, mock_sleep):
        """
        Check whether the tap backoffs properly for 5 times in case of MethodNotImplemented error.
        """
        mocked_google_ads_client = Mock()
        mocked_google_ads_client.search.side_effect = MethodNotImplemented("Not Implemented")

        try:
            make_request(mocked_google_ads_client, "", "")
        except MethodNotImplemented:
            pass

        # Verify that tap backoff for 5 times
        self.assertEqual(mocked_google_ads_client.search.call_count, 5)

    def test_502_bad_gaetway_error(self, mock_sleep):
        """
        Check whether the tap backoffs properly for 5 times in case of BadGateway error.
        """
        mocked_google_ads_client = Mock()
        mocked_google_ads_client.search.side_effect = BadGateway("Bad Gateway")

        try:
            make_request(mocked_google_ads_client, "", "")
        except BadGateway:
            pass

        # Verify that tap backoff for 5 times
        self.assertEqual(mocked_google_ads_client.search.call_count, 5)

    def test_503_service_unavailable_error(self, mock_sleep):
        """
        Check whether the tap backoffs properly for 5 times in case of ServiceUnavailable error.
        """
        mocked_google_ads_client = Mock()
        mocked_google_ads_client.search.side_effect = ServiceUnavailable("Service Unavailable")

        try:
            make_request(mocked_google_ads_client, "", "")
        except ServiceUnavailable:
            pass

        # Verify that tap backoff for 5 times
        self.assertEqual(mocked_google_ads_client.search.call_count, 5)

    def test_504_gateway_timeout_error(self, mock_sleep):
        """
        Check whether the tap backoffs properly for 5 times in case of GatewayTimeout error.
        """
        mocked_google_ads_client = Mock()
        mocked_google_ads_client.search.side_effect = GatewayTimeout("GatewayTimeout")

        try:
            make_request(mocked_google_ads_client, "", "")
        except GatewayTimeout:
            pass

        # Verify that tap backoff for 5 times
        self.assertEqual(mocked_google_ads_client.search.call_count, 5)

    def test_429_too_may_request_error(self, mock_sleep):
        """
        Check whether the tap backoffs properly for 5 times in case of TooManyRequests error.
        """
        mocked_google_ads_client = Mock()
        mocked_google_ads_client.search.side_effect = TooManyRequests("Resource has been exhausted")

        try:
            make_request(mocked_google_ads_client, "", "")
        except TooManyRequests:
            pass

        # Verify that tap backoff for 5 times
        self.assertEqual(mocked_google_ads_client.search.call_count, 5)

    def test_read_timeout_error(self, mock_sleep):
        """
        Check whether the tap backoffs properly for 5 times in case of ReadTimeout error.
        """
        mocked_google_ads_client = Mock()
        mocked_google_ads_client.search.side_effect = ReadTimeout("HTTPSConnectionPool(host='tap-tester-api.sandbox.stitchdata.com', port=443)")

        try:
            make_request(mocked_google_ads_client, "", "")
        except ReadTimeout:
            pass

        # Verify that tap backoff for 5 times
        self.assertEqual(mocked_google_ads_client.search.call_count, 5)

if __name__ == '__main__':
    unittest.main()
