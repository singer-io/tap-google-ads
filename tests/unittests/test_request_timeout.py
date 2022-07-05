import unittest
from tap_google_ads.streams import get_request_timeout

config_no_timeout = {}
config_int_timeout = {"request_timeout": 100}
config_float_timeout = {"request_timeout": 100.0}
config_str_timeout = {"request_timeout": "100"}
config_empty_str_timeout = {"request_timeout": ""}


class TestGetRequestTimeout(unittest.TestCase):

    def test_no_timeout(self):
        actual = get_request_timeout(config_no_timeout)
        expected = 900
        self.assertEqual(expected, actual)

    def test_valid_timeout(self):
        actual = get_request_timeout(config_int_timeout)
        expected = 100
        self.assertEqual(expected, actual)

    def test_string_timeout(self):
        actual = get_request_timeout(config_str_timeout)
        expected = 100
        self.assertEqual(expected, actual)

    def test_empty_string_timeout(self):
        actual = get_request_timeout(config_empty_str_timeout)
        expected = 900
        self.assertEqual(expected, actual)

    def test_float_timeout(self):
        actual = get_request_timeout(config_float_timeout)
        expected = 100
        self.assertEqual(expected, actual)

if __name__ == '__main__':
    unittest.main()
