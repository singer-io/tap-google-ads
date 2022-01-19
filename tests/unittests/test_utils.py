import unittest
from tap_google_ads.reports import flatten
from tap_google_ads.reports import make_field_names



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


class TestMakeFieldNames(unittest.TestCase):
    def test_single_word(self):
        actual = make_field_names("resource", ["type"])
        expected = ["resource.type"]
        self.assertListEqual(expected, actual)

    def test_dotted_field(self):
        actual = make_field_names("resource", ["tracking_setting.tracking_url"])
        expected = ["resource.tracking_setting.tracking_url"]
        self.assertListEqual(expected, actual)

    def test_foreign_key_field(self):
        actual = make_field_names("resource", ["customer_id", "accessible_bidding_strategy_id"])
        expected = ["customer.id", "accessible_bidding_strategy.id"]
        self.assertListEqual(expected, actual)

    def test_trailing_id_field(self):
        actual = make_field_names("resource", ["owner_customer_id"])
        expected = ["resource.owner_customer_id"]
        self.assertListEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
