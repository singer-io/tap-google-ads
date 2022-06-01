import unittest
from tap_google_ads.streams import create_core_stream_query

SELECTED_FIELDS = ["id"]
RESOURCE_NAME = "ads"

class TestFullTableQuery(unittest.TestCase):
    """
    Test that `create_core_stream_query` function build appropriate query with WHERE, ORDER BY clause.
    """
    def test_empty_filter_params_clause(self):
        """
        Verify that query does not contain WHERE and ORDER BY clause if filter_params value is None.
        """

        filter_params = []
        last_evaluated_key = {}

        expected = 'SELECT id1,id2 FROM ads  PARAMETERS omit_unselected_resource_names=true'

        actual = create_core_stream_query(RESOURCE_NAME, SELECTED_FIELDS, last_evaluated_key, filter_params)

        self.assertEqual(expected, actual)

    def test_empty_where_clause(self):
        """
        Verify that query contain only ORDER BY clause if filter_params value is not None and
        last_pk_fetched is empty.(Fresh sync)
        """
        filter_params = ['id1']
        last_evaluated_key = {}

        expected = 'SELECT id1,id2 FROM ads ORDER BY id1 ASC PARAMETERS omit_unselected_resource_names=true'

        actual = create_core_stream_query(RESOURCE_NAME, SELECTED_FIELDS, last_evaluated_key, filter_params)

        self.assertEqual(expected, actual)

    def test_where_orderby_clause_composite_pks(self):
        """
        Verify that query contains WHERE(inclusive) and ORDER BY clause if filter_params and
        last_pk_fetched are available. (interrupted sync). WHERE clause must have equality if stream contain
        a composite primary key.
        """
        filter_params = ['id1', 'id2']
        last_evaluated_key = {'id1': 4, 'id2': 5}

        expected = 'SELECT id1,id2 FROM ads WHERE id1 >= 4 ORDER BY id1, id2 ASC PARAMETERS omit_unselected_resource_names=true'

        actual = create_core_stream_query(RESOURCE_NAME, SELECTED_FIELDS, last_evaluated_key, filter_params)

        self.assertEqual(expected, actual)

    def test_where_orderby_clause_non_composite_pks(self):
        """
        Verify that query contains WHERE(exclusive) and ORDER BY clause if filter_params and
        last_pk_fetched are available. (interrupted sync). WHERE clause must exclude equality if stream does not contain
        a composite primary key.
        """
        filter_params = ['id1']
        last_evaluated_key = {'id1': 4}

        expected = 'SELECT id1,id2 FROM ads WHERE id1 > 4 ORDER BY id1 ASC PARAMETERS omit_unselected_resource_names=true'

        actual = create_core_stream_query(RESOURCE_NAME, SELECTED_FIELDS, last_evaluated_key, filter_params)

        self.assertEqual(expected, actual)
