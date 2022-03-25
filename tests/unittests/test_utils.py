import unittest
from tap_google_ads.streams import generate_hash
from tap_google_ads.streams import get_query_date
from tap_google_ads.streams import create_nested_resource_schema
from tap_google_ads.sync import shuffle
from tap_google_ads.sync import sort_selected_streams
from tap_google_ads.sync import sort_customers
from singer import metadata
from singer.utils import strptime_to_utc

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
    
    test_record_new_date = {
        'id': 1234567890,
        'currency_code': 'USD',
        'time_zone': 'America/New_York',
        'auto_tagging_enabled': False,
        'manager': False,
        'test_account': False,
        'impressions': 0,
        'interactions': 0,
        'invalid_clicks': 0,
        'date': '2022-01-20',
    }
    
    test_record_euro = {
        'id': 1234567890,
        'currency_code': 'EUR',
        'time_zone': 'Europe/Paris',
        'auto_tagging_enabled': False,
        'manager': False,
        'test_account': False,
        'impressions': 0,
        'interactions': 0,
        'invalid_clicks': 0,
        'date': '2022-01-19',
    }
    
    test_record_with_non_zero_metrics = {
        'id': 1234567890,
        'currency_code': 'USD',
        'time_zone': 'America/New_York',
        'auto_tagging_enabled': False,
        'manager': False,
        'test_account': False,
        'impressions': 10,
        'interactions': 10,
        'invalid_clicks': 10,
        'date': '2022-01-19',
    }
    
    test_record_without_metrics = {
        'id': 1234567890,
        'currency_code': 'USD',
        'time_zone': 'America/New_York',
        'auto_tagging_enabled': False,
        'manager': False,
        'test_account': False,
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

    expected_hash = '38d95857633f1e04092f7a308f0d3777d965cba80a5593803dd2b7e4a484ce64'

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
        
    def test_record_hash_is_same_with_metrics_selected_and_no_metrics_selected(self):
        self.assertEqual(self.expected_hash, generate_hash(self.test_record_without_metrics, self.test_metadata))
        self.assertEqual(self.expected_hash, generate_hash(self.test_record, self.test_metadata))
        
    def test_record_hash_is_same_with_metric_value_change(self):
        hash_non_zero_metrics = generate_hash(self.test_record_with_non_zero_metrics, self.test_metadata)
        hash_zero_metrics = generate_hash(self.test_record, self.test_metadata)
        self.assertEqual(hash_zero_metrics, hash_non_zero_metrics)

    def test_record_hash_is_different_with_different_segment_value(self):
        hash_record_orignal_date = generate_hash(self.test_record, self.test_metadata)
        hash_record_next_day = generate_hash(self.test_record_new_date, self.test_metadata)
        self.assertNotEqual(hash_record_orignal_date, hash_record_next_day)
        
    def test_record_hash_is_different_with_different_attribute_value(self):
        hash_record_usd = generate_hash(self.test_record, self.test_metadata)
        hash_record_euro = generate_hash(self.test_record_euro, self.test_metadata)
        self.assertNotEqual(hash_record_usd, hash_record_euro)
        
        
class TestGetQueryDate(unittest.TestCase):
    def test_one(self):
        """Given:
        - Start date before the conversion window
        - No bookmark

        return the start date"""
        actual = get_query_date(
            start_date="2022-01-01T00:00:00Z",
            bookmark=None,
            conversion_window_date="2022-01-23T00:00:00Z"
        )
        expected = strptime_to_utc("2022-01-01T00:00:00Z")
        self.assertEqual(expected, actual)

    def test_two(self):
        """Given:
        - Start date before the conversion window
        - bookmark after the conversion window

        return the conversion window"""
        actual = get_query_date(
            start_date="2022-01-01T00:00:00Z",
            bookmark="2022-02-01T00:00:00Z",
            conversion_window_date="2022-01-23T00:00:00Z"
        )
        expected = strptime_to_utc("2022-01-23T00:00:00Z")
        self.assertEqual(expected, actual)

    def test_three(self):
        """Given:
        - Start date after the conversion window
        - no bookmark

        return the start date"""
        actual = get_query_date(
            start_date="2022-02-01T00:00:00Z",
            bookmark=None,
            conversion_window_date="2022-01-23T00:00:00Z"
        )
        expected = strptime_to_utc("2022-02-01T00:00:00Z")
        self.assertEqual(expected, actual)

    def test_four(self):
        """Given:
        - Start date after the conversion window
        - bookmark after the start date

        return the start date"""
        actual = get_query_date(
            start_date="2022-02-01T00:00:00Z",
            bookmark="2022-02-08T00:00:00Z",
            conversion_window_date="2022-01-23T00:00:00Z"
        )
        expected = strptime_to_utc("2022-02-01T00:00:00Z")
        self.assertEqual(expected, actual)

    def test_five(self):
        """Given:
        - Start date before the conversion window
        - bookmark after the start date and before the conversion window

        return the bookmark"""
        actual = get_query_date(
            start_date="2022-01-01T00:00:00Z",
            bookmark="2022-01-14T00:00:00Z",
            conversion_window_date="2022-01-23T00:00:00Z"
        )
        expected = strptime_to_utc("2022-01-14T00:00:00Z")
        self.assertEqual(expected, actual)


class TestShuffleStreams(unittest.TestCase):
    selected_streams = [
        {"tap_stream_id": "stream1"},
        {"tap_stream_id": "stream2"},
        {"tap_stream_id": "stream3"},
        {"tap_stream_id": "stream4"},
        {"tap_stream_id": "stream5"},
    ]

    def test_shuffle_first_stream(self):
        actual = shuffle(
            self.selected_streams,
            "tap_stream_id",
            "stream1",
            sort_function=sort_selected_streams
        )
        expected = [
            {"tap_stream_id": "stream1"},
            {"tap_stream_id": "stream2"},
            {"tap_stream_id": "stream3"},
            {"tap_stream_id": "stream4"},
            {"tap_stream_id": "stream5"},
        ]
        self.assertListEqual(expected, actual)


    def test_shuffle_middle_stream(self):
        actual = shuffle(
            self.selected_streams,
            "tap_stream_id",
            "stream3",
            sort_function=sort_selected_streams
        )
        expected = [
            {"tap_stream_id": "stream3"},
            {"tap_stream_id": "stream4"},
            {"tap_stream_id": "stream5"},
            {"tap_stream_id": "stream1"},
            {"tap_stream_id": "stream2"},
        ]
        self.assertListEqual(expected, actual)

    def test_shuffle_last_stream(self):
        actual = shuffle(
            self.selected_streams,
            "tap_stream_id",
            "stream5",
            sort_function=sort_selected_streams
        )
        expected = [
            {"tap_stream_id": "stream5"},
            {"tap_stream_id": "stream1"},
            {"tap_stream_id": "stream2"},
            {"tap_stream_id": "stream3"},
            {"tap_stream_id": "stream4"},
        ]
        self.assertListEqual(expected, actual)

    def test_shuffle_deselect_currently_syncing(self):
        actual = shuffle(
            [
                {"tap_stream_id": "stream1"},
                {"tap_stream_id": "stream2"},
                {"tap_stream_id": "stream4"},
                {"tap_stream_id": "stream5"},
            ],
            "tap_stream_id",
            "stream3",
            sort_function=sort_selected_streams
        )
        expected = [
            {"tap_stream_id": "stream4"},
            {"tap_stream_id": "stream5"},            
            {"tap_stream_id": "stream1"},
            {"tap_stream_id": "stream2"},

        ]
        self.assertListEqual(expected, actual)


class TestShuffleCustomers(unittest.TestCase):

    customers = [
        {"customerId": "customer1"},
        {"customerId": "customer2"},
        {"customerId": "customer3"},
        {"customerId": "customer4"},
        {"customerId": "customer5"},
    ]

    def test_shuffle_first_customer(self):
        actual = shuffle(
            self.customers,
            "customerId",
            "customer1",
            sort_function=sort_customers
        )
        expected = [
            {"customerId": "customer1"},
            {"customerId": "customer2"},
            {"customerId": "customer3"},
            {"customerId": "customer4"},
            {"customerId": "customer5"},
        ]
        self.assertListEqual(expected, actual)

    def test_shuffle_middle_customer(self):
        actual = shuffle(
            self.customers,
            "customerId",
            "customer3",
            sort_function=sort_customers
        )
        expected = [
            {"customerId": "customer3"},
            {"customerId": "customer4"},
            {"customerId": "customer5"},
            {"customerId": "customer1"},
            {"customerId": "customer2"},
        ]
        self.assertListEqual(expected, actual)

    def test_shuffle_last_customer(self):
        actual = shuffle(
            self.customers,
            "customerId",
            "customer5",
            sort_function=sort_customers
        )
        expected = [
            {"customerId": "customer5"},
            {"customerId": "customer1"},
            {"customerId": "customer2"},
            {"customerId": "customer3"},
            {"customerId": "customer4"},
        ]
        self.assertListEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
