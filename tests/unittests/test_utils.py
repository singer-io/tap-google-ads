import unittest
from tap_google_ads.reports import flatten


class TestFlatten(unittest.TestCase):
    def test_flatten_one_level(self):
        nested_obj = {"a": {"b": "c"}, "d": "e"}
        actual = flatten(nested_obj)
        expected = {"a.b": "c", "d": "e"}
        self.assertDictEqual(expected, actual)

    def test_flatten_two_levels(self):
        nested_obj = {"a": {"b": {"c": "d", "e": "f"}, "g": "h"}}
        actual = flatten(nested_obj)
        expected = {"a.b.c": "d", "a.b.e": "f", "a.g": "h"}
        self.assertDictEqual(expected, actual)
