"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
import json
from datetime import timedelta
from datetime import datetime as dt

from tap_tester import connections, menagerie, runner


class GoogleAdsBase(unittest.TestCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """
    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    REPLICATION_KEY_FORMAT = "%Y-%m-%dT00:00:00.000000Z"

    start_date = ""

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-google-ads"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.google-ads"

    def get_customer_ids(self):
        return [
            os.getenv('TAP_GOOGLE_ADS_CUSTOMER_ID'),
            os.getenv('TAP_GOOGLE_ADS_LOGIN_CUSTOMER_ID'),
        ]

    def get_properties(self, original: bool = True):
        """Configurable properties, with a switch to override the 'start_date' property"""
        return_value = {
            'start_date':   '2021-12-01T00:00:00Z',
            'user_id':      'not used?', # TODO ?
            'customer_ids': ','.join(self.get_customer_ids()),
            # 'conversion_window_days': '30',
            'login_customer_ids': [{"customerId": os.getenv('TAP_GOOGLE_ADS_CUSTOMER_ID'),
                                    "loginCustomerId": os.getenv('TAP_GOOGLE_ADS_LOGIN_CUSTOMER_ID'),}],
        }

        # TODO_TDL-17911 Add a test around conversion_window_days
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def get_credentials(self):
        return {'developer_token': os.getenv('TAP_GOOGLE_ADS_DEVELOPER_TOKEN'),
                'oauth_client_id': os.getenv('TAP_GOOGLE_ADS_OAUTH_CLIENT_ID'),
                'oauth_client_secret': os.getenv('TAP_GOOGLE_ADS_OAUTH_CLIENT_SECRET'),
                'refresh_token':     os.getenv('TAP_GOOGLE_ADS_REFRESH_TOKEN')}

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        # TODO Investigate the foreign key expectations here,
        #       - must prove each uncommented entry is a true foregin key constraint.
        #       - must prove each commented entry is a NOT true foregin key constraint.
        return {
            # Core Objects
            "accounts": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.FOREIGN_KEYS: set(),
            },
            "campaigns": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.FOREIGN_KEYS: {
                    # 'accessible_bidding_strategy_id',
                    # 'bidding_strategy_id',
                    # 'campaign_budget_id',
                    'customer_id'
                },
            },
            "ad_groups": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.FOREIGN_KEYS: {
                    # 'accessible_bidding_strategy_id',
                    # 'bidding_strategy_id',
                    'campaign_id',
                    'customer_id',
                },
            },
            "ads": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.FOREIGN_KEYS: {
                    "campaign_id",
                    "customer_id",
                    "ad_group_id"
                },
            },
            'campaign_budgets': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.FOREIGN_KEYS: {
                    "customer_id",
                    "campaign_id",
                },
            },
            'bidding_strategies': {
                self.PRIMARY_KEYS:{"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.FOREIGN_KEYS: {"customer_id"},
            },
            'accessible_bidding_strategies': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.FOREIGN_KEYS: {"customer_id"},
            },
            # Report objects
            "age_range_performance_report": {  # "age_range_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "campaign_performance_report": {  # "campaign"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "campaign_audience_performance_report": {  # "campaign_audience_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            # TODO Post Alpha
            # "call_metrics_call_details_report": {  # "call_view"
            #     self.PRIMARY_KEYS: {"_sdc_record_hash"},
            #     self.REPLICATION_METHOD: self.INCREMENTAL,
            #     self.REPLICATION_KEYS: {"date"},
            # },
            "click_performance_report": { #  "click_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "display_keyword_performance_report": {  # "display_keyword_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "display_topics_performance_report": {  # "topic_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "gender_performance_report": {  # "gender_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "geo_performance_report": {  # "geographic_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "user_location_performance_report": {  # "user_location_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "keywordless_query_report": {  # "dynamic_search_ads_search_term_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "keywords_performance_report": {  # "keyword_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "landing_page_report": {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "expanded_landing_page_report": {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "placeholder_feed_item_report": {  # "feed_item", "feed_item_target"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "placeholder_report": { # "feed_placeholder_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "placement_performance_report": {  # "managed_placement_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "search_query_performance_report": {  # "search_term_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "shopping_performance_report": {  # "shopping_performance_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "user_location_performance_report": {  # "user_location_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "video_performance_report": {  # "video"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "account_performance_report": { # accounts
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "ad_group_performance_report": {  # ad_group
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "ad_group_audience_performance_report": {  # ad_group_audience_view
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            "ad_performance_report": {  # ads
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
            },
            # "criteria_performance_report": { # DEPRECATED TODO maybe possilbe?
            #     self.PRIMARY_KEYS: {"TODO"},
            #     self.REPLICATION_METHOD: self.INCREMENTAL,
            #     self.REPLICATION_KEYS: {"date"},
            # },
            # "final_url_report": {  # DEPRECATED Replaced with landing page / expanded landing page
            #     self.PRIMARY_KEYS: {},
            #     self.REPLICATION_METHOD: self.INCREMENTAL,
            #     self.REPLICATION_KEYS: {"date"},
            # },
            # Custom Reports TODO feature
        }

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    # TODO confirm whether or not these apply for
    #   core objects ?
    #   report objects ?
    # def child_streams(self):
    #     """
    #     Return a set of streams that are child streams
    #     based on having foreign key metadata
    #     """
    #     return {stream for stream, metadata in self.expected_metadata().items()
    #             if metadata.get(self.FOREIGN_KEYS)}

    def expected_foreign_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of foreign key fields
        """
        return {table: properties.get(self.FOREIGN_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) | \
                v.get(self.FOREIGN_KEYS, set())

        return auto_fields

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}


    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_ADWORDS_DEVELOPER_TOKEN'),
                                    os.getenv('TAP_ADWORDS_OAUTH_CLIENT_ID'),
                                    os.getenv('TAP_ADWORDS_OAUTH_CLIENT_SECRET'),
                                    os.getenv('TAP_ADWORDS_REFRESH_TOKEN'),
                                    os.getenv('TAP_ADWORDS_CUSTOMER_IDS')] if x == None]
        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))


    #########################
    #   Helper Methods      #
    #########################

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = {found_catalog['stream_name'] for found_catalog in found_catalogs}
        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count


    # TODO we may need to account for exclusion rules
    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs,
                                                     select_default_fields: bool = True,
                                                     select_pagination_fields: bool = False):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters. Note that selecting all fields is not
        possible for this tap due to dimension/metric conflicts set by Google and
        enforced by the Stitch UI.

        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(conn_id, test_catalogs, True)
        # self._select_streams_and_fields(
        #     conn_id=conn_id, catalogs=test_catalogs,
        #     select_default_fields=select_default_fields,
        #     select_pagination_fields=select_pagination_fields
        # )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected_streams = [tc.get('stream_name') for tc in test_catalogs]
        expected_default_fields = self.expected_default_fields()
        expected_pagination_fields = self.expected_pagination_fields()
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all intended streams are selected
            selected = catalog_entry['metadata'][0]['metadata'].get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected_streams:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            # collect field selection expecationas
            expected_automatic_fields = self.expected_automatic_fields()[cat['stream_name']]
            selected_default_fields = expected_default_fields[cat['stream_name']] if select_default_fields else set()
            selected_pagination_fields = expected_pagination_fields[cat['stream_name']] if select_pagination_fields else set()

            # Verify all intended fields within the stream are selected
            expected_selected_fields = expected_automatic_fields | selected_default_fields | selected_pagination_fields
            selected_fields = self._get_selected_fields_from_metadata(catalog_entry['metadata'])
            for field in expected_selected_fields:
                field_selected = field in selected_fields
                print("\tValidating field selection on {}.{}: {}".format(cat['stream_name'], field, field_selected))
            self.assertSetEqual(expected_selected_fields, selected_fields)

    @staticmethod
    def _get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)
    @staticmethod
    def deselect_streams(conn_id, catalogs):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            connections.deselect_catalog_via_metadata(conn_id, catalog, schema)

    def _select_streams_and_fields(self, conn_id, catalogs, select_default_fields):
        """Select all streams and all fields within streams"""

        for catalog in catalogs:

            schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            metadata = schema_and_metadata['metadata']

            properties = set(md['breadcrumb'][-1] for md in metadata
                             if len(md['breadcrumb']) > 0 and md['breadcrumb'][0] == 'properties')

            # get a list of all properties so that none are selected
            if select_default_fields:
                non_selected_properties = properties.difference(
                    self.expected_default_fields()[catalog['stream_name']]
                )
            else:
                non_selected_properties = properties

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema_and_metadata, [], non_selected_properties)

    @staticmethod
    def parse_date(date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d"
        }
        for date_format in date_formats:
            try:
                date_stripped = dt.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError("Tests do not account for dates of this format: {}".format(date_value))

    def timedelta_formatted(self, dtime, days=0):
        """Convert a string formatted datetime to a new string formatted datetime with a timedelta applied."""
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            try:
                date_stripped = dt.strptime(dtime, self.REPLICATION_KEY_FORMAT)
                return_date = date_stripped + timedelta(days=days)

                return dt.strftime(return_date, self.REPLICATION_KEY_FORMAT)

            except ValueError:
                return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))

    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################

    def select_all_streams_and_default_fields(self, conn_id, catalogs):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            if not self.is_report(catalog['tap_stream_id']):
                raise RuntimeError("Method intended for report streams only.")

            schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            metadata = schema_and_metadata['metadata']
            properties = {md['breadcrumb'][-1]
                          for md in metadata
                          if len(md['breadcrumb']) > 0 and md['breadcrumb'][0] == 'properties'}
            expected_fields = self.expected_default_fields()[catalog['stream_name']]
            self.assertTrue(expected_fields.issubset(properties),
                            msg=f"{catalog['stream_name']} missing {expected_fields.difference(properties)}")
            non_selected_properties = properties.difference(expected_fields)
            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema_and_metadata, [], non_selected_properties
            )

    def select_stream_and_specified_fields(self, conn_id, catalog, fields_to_select: set()):
        """
        Select the specified stream and it's fields.
        Intended only for report streams.
        """
        if not self.is_report(catalog['tap_stream_id']):
            raise RuntimeError("Method intended for report streams only.")

        schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
        metadata = schema_and_metadata['metadata']
        properties = {md['breadcrumb'][-1]
                      for md in metadata
                      if len(md['breadcrumb']) > 0 and md['breadcrumb'][0] == 'properties'}
        self.assertTrue(fields_to_select.issubset(properties),
                        msg=f"{catalog['stream_name']} missing {fields_to_select.difference(properties)}")
        non_selected_properties = properties.difference(fields_to_select)
        connections.select_catalog_and_fields_via_metadata(
            conn_id, catalog, schema_and_metadata, [], non_selected_properties
        )

    def is_report(self, stream):
        return stream.endswith('_report')

    # TODO exclusion rules

    @staticmethod
    def expected_default_fields():
        """
        Report streams will select fields based on the default values that
        are provided when selecting the report type in Google's UI when possible.
        These fields do not translate perfectly to our report syncs and so a subset
        of those fields are used in almost all cases here.

        returns a dictionary of reports to standard fields
        """
        return {
            'ad_performance_report': {
                'average_cpc',  # 'Avg. CPC',
                'clicks',  # 'Clicks',
                'conversions',  # 'Conversions',
                'cost_per_conversion',  # 'Cost / conv.',
                'ctr',  # 'CTR',
                'customer_id',  # 'Customer ID',
                'impressions',  # 'Impr.',
                'view_through_conversions',  # 'View-through conv.',
            },
            "ad_group_performance_report": {
                'average_cpc',  # Avg. CPC,
                'clicks',  # Clicks,
                'conversions',  # Conversions,
                'cost_per_conversion',  # Cost / conv.,
                'ctr',  # CTR,
                'customer_id',  # Customer ID,
                'impressions',  # Impr.,
                'view_through_conversions',  # View-through conv.,
            },
            "ad_group_audience_performance_report": {
                'ad_group_name',
            },
            "campaign_performance_report": {
                'average_cpc',  # Avg. CPC,
                'clicks',  # Clicks,
                'conversions',  # Conversions,
                'cost_per_conversion',  # Cost / conv.,
                'ctr',  # CTR,
                'customer_id',  # Customer ID,
                'impressions',  # Impr.,
                'view_through_conversions',  # View-through conv.,
            },
            "click_performance_report": {
                'ad_group_ad',
                'ad_group_id',
                'ad_group_name',
                'ad_group_status',
                'ad_network_type',
                'click_view_area_of_interest',
                'campaign_location_target',
                'click_type',
                'clicks',
                'customer_descriptive_name',
                'customer_id',
                'device',
                'click_view_gclid',
                'click_view_location_of_presence',
                'month_of_year',
                'click_view_page_number',
                'slot',
                'click_view_user_list',
            },
            "display_keyword_performance_report": { # TODO NO DATA AVAILABLE
                'ad_group_name',
                # 'average_cpc',  # Avg. CPC,
                # 'average_cpm',  # Avg. CPM,
                # 'average_cpv',  # Avg. CPV,
                'clicks',  # Clicks,
                # 'conversions',  # Conversions,
                # 'cost_per_conversion',  # Cost / conv.,
                'impressions',  # Impr.,
                # 'interaction_rate',  # Interaction rate,
                # 'interactions',  # Interactions,
                # 'view_through_conversions',  # View-through conv.,
            },
            "display_topics_performance_report": { # TODO NO DATA AVAILABLE
                'ad_group_name', # 'ad_group',  # Ad group,
                'average_cpc',  # Avg. CPC,
                'average_cpm',  # Avg. CPM,
                'campaign_name',  # 'campaign',  # Campaign,
                'clicks',  # Clicks,
                'ctr',  # CTR,
                'customer_currency_code',  # 'currency_code',  # Currency code,
                'impressions',  # Impr.,
            },
            "placement_performance_report": { # TODO NO DATA AVAILABLE
                'clicks',
                'impressions',
                'ad_group_id',
                'ad_group_criterion_placement',  # 'placement_group', 'placement_type',
            },
            "keywords_performance_report": { # TODO NO DATA AVAILABLE
                'campaign_id',
                'clicks',
                'impressions',
                'ad_group_criterion_keyword',
            },
            "keywordless_query_report": {
                'campaign_id',
                'clicks',
                'impressions',
            },
            # "shopping_performance_report": set(),
            "video_performance_report": {
                'campaign_name',
                'clicks',
                # 'video_views',
            },
            # NOTE AFTER THIS POINT COULDN"T FIND IN UI
            "account_performance_report": {
                'average_cpc',
                'click_type',
                'clicks',
                'date',
                'customer_descriptive_name',
                'customer_id',
                'impressions',
                'invalid_clicks',
                'customer_manager',
                'customer_test_account',
                'customer_time_zone',
            },
            "geo_performance_report": {
                'clicks',
                'ctr',  # CTR,
                'impressions',  # Impr.,
                'average_cpc',
                'conversions',
                'view_through_conversions',  # View-through conv.,
                'cost_per_conversion',  # Cost / conv.,
                'geo_target_region',
           },
            "gender_performance_report": {
                'ad_group_criterion_gender',
                'average_cpc',  # Avg. CPC,
                'clicks',  # Clicks,
                'conversions',  # Conversions,
                'cost_per_conversion',  # Cost / conv.,
                'ctr',  # CTR,
                'customer_id',  # Customer ID,
                'impressions',  # Impr.,
                'view_through_conversions',  # View-through conv.,
            },
            "search_query_performance_report": {
                'clicks',
                'ctr',  # CTR,
                'impressions',  # Impr.,
                'average_cpc',
                'conversions',
                'view_through_conversions',  # View-through conv.,
                'cost_per_conversion',  # Cost / conv.,
                'search_term_view_search_term',
                'search_term_match_type',
            },
            "age_range_performance_report": {
                'clicks',
                'ctr',  # CTR,
                'impressions',  # Impr.,
                'average_cpc',
                'conversions',
                'view_through_conversions',  # View-through conv.,
                'cost_per_conversion',  # Cost / conv.,
                'ad_group_criterion_age_range', # 'Age',
            },
            'placeholder_feed_item_report': {
                'clicks',
                'impressions',
                'feed_placeholder_view_placeholder_type',
            },
            'placeholder_report': {
                'clicks',
                'cost_micros',
                'interactions',
                'feed_placeholder_view_placeholder_type',
            },
            'user_location_performance_report': {
                'campaign_id',
                'clicks',
                'geo_target_region',
            },
            'landing_page_report': {
                'ad_group_name',
                'campaign_name',
                'clicks',
                'average_cpc',
                'landing_page_view_unexpanded_final_url',
            },
            'expanded_landing_page_report': {
                'ad_group_name',
                'campaign_name',
                'clicks',
                'average_cpc',
                'expanded_landing_page_view_expanded_final_url',
            },
            'campaign_audience_performance_report': {
                'campaign_name',
                'click_type',
                'clicks',
                'interactions',
            },
        }
    def assertIsDateFormat(self, value, str_format):
        """
        Assertion Method that verifies a string value is a formatted datetime with
        the specified format.
        """
        try:
            _ = dt.strptime(value, str_format)
        except ValueError as err:
            raise AssertionError(
                f"Value does not conform to expected format: {str_format}"
            ) from err
