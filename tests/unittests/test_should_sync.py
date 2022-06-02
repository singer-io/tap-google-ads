import unittest
from tap_google_ads.streams import should_sync_record

RECORD = {
    "id1": 1,
    "id2": 2
}
class TestShouldSyncMethod(unittest.TestCase):
    """
    Verify that tap should skip the already synced records for streams that have a composite primary key.
    """
    def test_empty_last_pk_fetched_single_pk(self):
        """
        Verify that tap sync the record(Return True) for fresh sync(i.e. State is empty) in case of stream which has a single primary key.
        """
        filter_param = ["ads.id1"]
        pks = ["id1"]
        last_pk_fetched = {}
        expected = True

        actual = should_sync_record(RECORD, filter_param, pks, last_pk_fetched)

        self.assertEqual(expected, actual)

    def test_empty_last_pk_fetched_composite_pk(self):
        """
        Verify that tap sync the record(Return True) for fresh sync(i.e. State is empty) in case of a stream which has a composite primary key.
        """
        filter_param = ["ads.id1", "ads.id2"]
        pks = ["id1", "id2"]
        last_pk_fetched = {}
        expected = True

        actual = should_sync_record(RECORD, filter_param, pks, last_pk_fetched)

        self.assertEqual(expected, actual)

    def test_non_empty_last_pk_fetched_single_pk(self):
        """
        Verify that tap sync the new record(Return True) for interrupted sync(i.e. State is nonempty) in case of stream which has a single primary key.
        """
        filter_param = ["ads.id1"]
        pks = ["id1"]
        last_pk_fetched = {"id1": 1}
        expected = True

        actual = should_sync_record(RECORD, filter_param, pks, last_pk_fetched)

        self.assertEqual(expected, actual)

    def test_non_empty_last_pk_fetched_composite_pk(self):
        """
        Verify that tap sync the new record(Return True) for interrupted sync(i.e. State is nonempty) in case of a stream which has a composite primary key.
        """
        filter_param = ["ads.id1", "ads.id2"]
        pks = ["id1", "id2"]
        last_pk_fetched = {"ads.id1": 1, "ads.id2": 1}
        expected = True

        actual = should_sync_record(RECORD, filter_param, pks, last_pk_fetched)

        self.assertEqual(expected, actual)

    def test_non_empty_last_pk_fetched_composite_pk(self):
        """
        Verify that tap skips the old record(Return False) for interrupted sync(i.e. State is nonempty) in case of a stream that has a composite primary key.
        """
        filter_param = ["ads.id1", "ads.id2"]
        pks = ["id1", "id2"]
        last_pk_fetched = {"ads.id1": 1, "ads.id2": 3}
        expected = False

        actual = should_sync_record(RECORD, filter_param, pks, last_pk_fetched)

        self.assertEqual(expected, actual)
