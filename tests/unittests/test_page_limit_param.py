import unittest
from tap_google_ads.sync import get_page_limit, DEFAULT_PAGE_LIMIT


def get_config(value):
    return {
        "limit": value
        }

class TestPageLimitParam(unittest.TestCase):

    """Tests to validate the page_limit parameter"""

    def test_integer_page_limit_field(self):
        expected_value = 100
        actual_value = get_page_limit(get_config(100))
        
        self.assertEqual(actual_value, expected_value)

    def test_float_page_limit_field(self):
        expected_value = 100
        actual_value = get_page_limit(get_config(100.05))

        self.assertEqual(actual_value, expected_value)
    
    def test_zero_int_page_limit_field(self):
        expected_value = DEFAULT_PAGE_LIMIT
        actual_value = get_page_limit(get_config(0))

        self.assertEqual(actual_value, expected_value)

    def test_zero_float_page_limit_field(self):
        expected_value = DEFAULT_PAGE_LIMIT
        actual_value = get_page_limit(get_config(0.5))

        self.assertEqual(actual_value, expected_value)

    def test_empty_string_page_limit_field(self):
        expected_value = DEFAULT_PAGE_LIMIT
        actual_value = get_page_limit(get_config(""))

        self.assertEqual(actual_value, expected_value)

    def test_string_page_limit_field(self):
        expected_value = 100
        actual_value = get_page_limit(get_config("100"))

        self.assertEqual(actual_value, expected_value)

    def test_invalid_string_page_limit_field(self):
        expected_value = DEFAULT_PAGE_LIMIT
        actual_value = get_page_limit(get_config("dg%#"))

        self.assertEqual(actual_value, expected_value)

    def test_nagative_int_page_limit_field(self):
        expected_value = DEFAULT_PAGE_LIMIT
        actual_value = get_page_limit(get_config(-10))

        self.assertEqual(actual_value, expected_value)

    def test_nagative_float_page_limit_field(self):
        expected_value = DEFAULT_PAGE_LIMIT
        actual_value = get_page_limit(get_config(-10.5))

        self.assertEqual(actual_value, expected_value)