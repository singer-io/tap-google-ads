from collections import namedtuple
import unittest
from tap_google_ads.discover import get_segments
from tap_google_ads.discover import get_attributes


RESOURCE_SCHEMA = {
    "template": {
        "category": "",
        "attributes": [],
        "segments": []
    },
    "resource1": {
        "category": "RESOURCE",
        "attributes": ["resource1.thing3", "resource1.thing4"],
        "segments": []
    }

}


class TestGetSegments(unittest.TestCase):
    def test_get_segments_on_a_non_resource(self):
        data_types = ["ATTRIBUTE", "SEGMENT", "METRIC"]
        for data_type in data_types:
            with self.subTest(data_type=data_type):
                resource = {
                    "category": data_type,
                    "attributes": ["attribute1"],
                    "segments": ["segment1"]
                }

                actual = get_segments(RESOURCE_SCHEMA, resource)

                expected = []

                self.assertListEqual(expected, actual)

    def test_get_segments_on_a_resource_with_only_dot_segments(self):
        resource = {
            "category": "RESOURCE",
            "attributes": ["attribute1"],
            "segments": ["segments.thing1", "segments.thing2"]
        }

        actual = get_segments(RESOURCE_SCHEMA, resource)

        expected = ["segments.thing1", "segments.thing2"]

        self.assertListEqual(expected, actual)

    def test_get_segments_on_a_resource_with_dot_segments_and_segmenting_resource(self):
        resource = {
            "category": "RESOURCE",
            "attributes": ["attribute1"],
            "segments": ["segments.thing1", "segments.thing2", "resource1"]
        }

        actual = get_segments(RESOURCE_SCHEMA, resource)

        expected = ["segments.thing1", "segments.thing2", "resource1.thing3", "resource1.thing4"]

        self.assertListEqual(expected, actual)


api_object = namedtuple("api_object", "category attribute_resources name")

class TestGetAttributes(unittest.TestCase):
    def test_get_attributes_on_a_non_resource(self):
        api_objects = [
            api_object(3, [], "resource.attr1"),
            api_object(3, [], "resource.attr2"),
            api_object(3, [], "resource.attr3"),
        ]
        data_types = [0, 1, 2, 3, 5, 6]
        for data_type in data_types:
            with self.subTest(data_type=data_type):
                resource = api_object(data_type, [], "resource.attr")
                actual = get_attributes(api_objects, resource)

                expected = []

                self.assertListEqual(expected, actual)

    def test_get_attributes_on_a_resource(self):
        api_objects = [
            api_object(3, [], "resource.attr1"),
            api_object(3, [], "resource.attr2"),
            api_object(3, [], "resource.attr3"),
        ]
        resource = api_object(2, [], "resource")

        actual = get_attributes(api_objects, resource)

        expected = ["resource.attr1", "resource.attr2", "resource.attr3"]

        self.assertListEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
