import unittest
from tap_google_ads.reports import generate_hash
from tap_google_ads.streams import create_nested_resource_schema
from singer import metadata


resource_schema = {
    "accessible_bidding_strategy.id": {"json_schema": {"type": ["null", "integer"]}},
    "accessible_bidding_strategy.strategy.id": {"json_schema": {"type": ["null", "integer"]}}
}
class TestCreateNestedResourceSchema(unittest.TestCase):

    def test_one(self):
        actual = create_nested_resource_schema(resource_schema, ["accessible_bidding_strategy.id"])
        expected = {
            "type": [
                "null",
                "object"
            ],
            "properties": {
                "accessible_bidding_strategy" : {
                    "type": ["null", "object"],
                    "properties": {
                        "id": {
                            "type": [
                                "null",
                                "integer"
                            ]
                        }
                    }
                }
            }
        }
        self.assertDictEqual(expected, actual)

    def test_two(self):
        actual = create_nested_resource_schema(resource_schema, ["accessible_bidding_strategy.strategy.id"])
        expected = {
            "type": ["null", "object"],
            "properties": {
                "accessible_bidding_strategy": {
                    "type": ["null", "object"],
                    "properties": {
                        "strategy": {
                            "type": ["null", "object"],
                            "properties": {
                                "id": {"type": ["null", "integer"]}
                            }
                        }
                    }
                }
            }
        }
        self.assertDictEqual(expected, actual)

    def test_siblings(self):
        actual = create_nested_resource_schema(
            resource_schema,
            ["accessible_bidding_strategy.id", "accessible_bidding_strategy.strategy.id"]
        )
        expected = {
            "type": ["null", "object"],
            "properties": {
                "accessible_bidding_strategy": {
                    "type": ["null", "object"],
                    "properties": {
                        "strategy": {
                            "type": ["null", "object"],
                            "properties": {
                                "id": {"type": ["null", "integer"]}
                            }
                        },
                        "id": {"type": ["null", "integer"]}
                    }
                }
            }
        }
        self.assertDictEqual(expected, actual)


class TestRecordHashing(unittest.TestCase):

    test_record = {
        'id': 1234567890,
        'currency_code': 'USD',
        'time_zone': 'America/New_York',
        'auto_tagging_enabled': False,
        'manager': False,
        'test_account': False,
        'impressions': 0,
        'interactions': 0,
        'invalid_clicks': 0,
        'date': '2022-01-19',
    }

    test_record_shuffled = {
        'currency_code': 'USD',
        'date': '2022-01-19',
        'auto_tagging_enabled': False,
        'time_zone': 'America/New_York',
        'test_account': False,
        'manager': False,
        'id': 1234567890,
        'interactions': 0,
        'invalid_clicks': 0,
        'impressions': 0,
    }

    test_metadata = metadata.to_list({
        ('properties', 'id'): {'behavior': 'ATTRIBUTE'},
        ('properties', 'currency_code'): {'behavior': 'ATTRIBUTE'},
        ('properties', 'time_zone'): {'behavior': 'ATTRIBUTE'},
        ('properties', 'auto_tagging_enabled'): {'behavior': 'ATTRIBUTE'},
        ('properties', 'manager'): {'behavior': 'ATTRIBUTE'},
        ('properties', 'test_account'): {'behavior': 'ATTRIBUTE'},
        ('properties', 'impressions'): {'behavior': 'METRIC'},
        ('properties', 'interactions'): {'behavior': 'METRIC'},
        ('properties', 'invalid_clicks'): {'behavior': 'METRIC'},
        ('properties', 'date'): {'behavior': 'SEGMENT'},
    })

    expected_hash = 'ade8240f134633fe125388e469e61ccf9e69033fd5e5f166b4b44766bc6376d3'

    def test_record_hash_canary(self):
        self.assertEqual(self.expected_hash, generate_hash(self.test_record, self.test_metadata))

    def test_record_hash_is_same_regardless_of_order(self):
        self.assertEqual(self.expected_hash, generate_hash(self.test_record, self.test_metadata))
        self.assertEqual(self.expected_hash, generate_hash(self.test_record_shuffled, self.test_metadata))

    def test_record_hash_is_same_with_fewer_metrics(self):
        test_record_fewer_metrics = dict(self.test_record)
        test_record_fewer_metrics.pop('interactions')
        test_record_fewer_metrics.pop('invalid_clicks')
        self.assertEqual(self.expected_hash, generate_hash(test_record_fewer_metrics, self.test_metadata))

    def test_record_hash_is_different_with_non_metric_value(self):
        test_diff_record = dict(self.test_record)
        test_diff_record['date'] = '2022-02-03'
        self.assertNotEqual(self.expected_hash, generate_hash(test_diff_record, self.test_metadata))


if __name__ == '__main__':
    unittest.main()
