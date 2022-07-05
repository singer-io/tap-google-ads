import unittest
from tap_google_ads.sync import get_query_limit, DEFAULT_QUERY_LIMIT


def get_config(value):
    return {
        "query_limit": value
        }

class TestQueryLimitParam(unittest.TestCase):

    """Tests to validate different values of the query_limit parameter"""

    def test_integer_query_limit_field(self):
        """ Verify that limit is set to 100 if int 100 is given in the config """
        expected_value = 100
        actual_value = get_query_limit(get_config(100))

        self.assertEqual(actual_value, expected_value)

    def test_float_query_limit_field(self):
        """ Verify that limit is set to 100 if float 100.05 is given in the config """

        expected_value = 100
        actual_value = get_query_limit(get_config(100.05))

        self.assertEqual(actual_value, expected_value)

    def test_zero_int_query_limit_field(self):
        """ Verify that limit is set to DEFAULT_QUERY_LIMIT if 0 is given in the config """

        expected_value = DEFAULT_QUERY_LIMIT
        actual_value = get_query_limit(get_config(0))

        self.assertEqual(actual_value, expected_value)

    def test_zero_float_query_limit_field(self):
        """ Verify that limit is set to DEFAULT_QUERY_LIMIT if 0.5 is given in the config """

        expected_value = DEFAULT_QUERY_LIMIT
        actual_value = get_query_limit(get_config(0.5))

        self.assertEqual(actual_value, expected_value)

    def test_empty_string_query_limit_field(self):
        """ Verify that limit is set to DEFAULT_QUERY_LIMIT if empty string is given in the config """

        expected_value = DEFAULT_QUERY_LIMIT
        actual_value = get_query_limit(get_config(""))

        self.assertEqual(actual_value, expected_value)

    def test_string_query_limit_field(self):
        """ Verify that limit is set to 100 if string "100" is given in the config """

        expected_value = 100
        actual_value = get_query_limit(get_config("100"))

        self.assertEqual(actual_value, expected_value)

    def test_invalid_string_query_limit_field(self):
        """ Verify that limit is set to DEFAULT_QUERY_LIMIT if invalid string is given in the config """

        expected_value = DEFAULT_QUERY_LIMIT
        actual_value = get_query_limit(get_config("dg%#"))

        self.assertEqual(actual_value, expected_value)

    def test_negative_int_query_limit_field(self):
        """ Verify that limit is set to 100 if negative int is given in the config """

        expected_value = DEFAULT_QUERY_LIMIT
        actual_value = get_query_limit(get_config(-10))

        self.assertEqual(actual_value, expected_value)

    def test_negative_float_query_limit_field(self):
        """ Verify that limit is set to 100 if negative float is given in the config """

        expected_value = DEFAULT_QUERY_LIMIT
        actual_value = get_query_limit(get_config(-10.5))

        self.assertEqual(actual_value, expected_value)
