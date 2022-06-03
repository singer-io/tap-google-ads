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

        filter_params = None
        last_pk_fetched = {}
        composite_pks = False

        expected_query = 'SELECT id FROM ads  PARAMETERS omit_unselected_resource_names=true'

        actual_query = create_core_stream_query(RESOURCE_NAME, SELECTED_FIELDS, last_pk_fetched, filter_params, composite_pks)

        self.assertEqual(expected_query, actual_query)

    def test_empty_where_clause(self):
        """
        Verify that query contain only ORDER BY clause if filter_params value is not None and
        last_pk_fetched is empty.(Fresh sync)
        """
        filter_params = 'id'
        last_pk_fetched = {}
        composite_pks = False
        expected_query = 'SELECT id FROM ads ORDER BY id ASC PARAMETERS omit_unselected_resource_names=true'

        actual_query = create_core_stream_query(RESOURCE_NAME, SELECTED_FIELDS, last_pk_fetched, filter_params, composite_pks)

        self.assertEqual(expected_query, actual_query)

    def test_where_orderby_clause_composite_pks(self):
        """
        Verify that query contains WHERE(inclusive) and ORDER BY clause if filter_params and
        last_pk_fetched are available. (interrupted sync). WHERE clause must have equality if stream contain
        a composite primary key.
        """
        filter_params = 'id'
        last_pk_fetched = 4
        composite_pks = True

        expected_query = 'SELECT id FROM ads WHERE id >= 4 ORDER BY id ASC PARAMETERS omit_unselected_resource_names=true'

        actual_query = create_core_stream_query(RESOURCE_NAME, SELECTED_FIELDS, last_pk_fetched, filter_params, composite_pks)

        self.assertEqual(expected_query, actual_query)

    def test_where_orderby_clause_non_composite_pks(self):
        """
        Verify that query contains WHERE(exclusive) and ORDER BY clause if filter_params and
        last_pk_fetched are available. (interrupted sync). WHERE clause must exclude equality if stream does not contain
        a composite primary key.
        """
        filter_params = 'id'
        last_pk_fetched = 4
        composite_pks = False

        expected_query = 'SELECT id FROM ads WHERE id > 4 ORDER BY id ASC PARAMETERS omit_unselected_resource_names=true'

        actual_query = create_core_stream_query(RESOURCE_NAME, SELECTED_FIELDS, last_pk_fetched, filter_params, composite_pks)

        self.assertEqual(expected_query, actual_query)
