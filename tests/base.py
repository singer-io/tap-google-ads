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
    AUTOMATIC_KEYS = "table-automatic-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    REPLICATION_KEY_FORMAT = "%Y-%m-%dT00:00:00.000000Z"

    start_date = ""
    end_date = "2022-03-15T00:00:00Z"

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
            'end_date': self.end_date,
            'user_id':      'not used?',  # Useless config property carried over from AdWords
            'customer_ids': ','.join(self.get_customer_ids()),
            'query_limit': 2,
            # 'conversion_window_days': '30',
            'login_customer_ids': [{"customerId": os.getenv('TAP_GOOGLE_ADS_CUSTOMER_ID'),
                                    "loginCustomerId": os.getenv('TAP_GOOGLE_ADS_LOGIN_CUSTOMER_ID'),}],
        }

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
        """
        The expected streams and metadata about the streams

        DEPRECATED reports from tap-adwords:
            "CRITERIA_PERFORMANCE_REPORT"
            "FINAL_URL_REPORT" replaced by landing page / expanded landing page
        """
        return {
            # Core Objects
            "accessible_bidding_strategies": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {"customer_id"},
            },
            "accounts": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: set(),
            },
            "ad_groups": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {
                    "campaign_id",
                    "customer_id",
                },
            },
            "ad_group_criterion": {
                self.PRIMARY_KEYS: {"ad_group_id", "criterion_id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {
                    "campaign_id",
                    "customer_id",
                },
            },
            "ads": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {
                    "ad_group_id",
                    "campaign_id",
                    "customer_id",
                },
            },
            "assets": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: set(),
            },
            "bidding_strategies": {
                self.PRIMARY_KEYS:{"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {"customer_id"},
            },
            "call_details": {
                self.PRIMARY_KEYS: {"resource_name"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {
                    "ad_group_id",
                    "campaign_id",
                    "customer_id"
                },
            },
            "campaigns": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {"customer_id"},
            },
            "campaign_budgets": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {"customer_id"},
            },
            "campaign_criterion": {
                self.PRIMARY_KEYS: {"campaign_id", "criterion_id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {"customer_id"},
            },
            "campaign_labels": {
                self.PRIMARY_KEYS: {"resource_name"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {
                    "customer_id",
                    "campaign_id",
                    "label_id"
                },
            },
            "carrier_constant": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: set(),
            },
            "labels": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {"customer_id"},
            },
            "language_constant": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: set(),
            },
            "mobile_app_category_constant": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: set(),
            },
            "mobile_device_constant": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: set(),
            },
            "operating_system_version_constant": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: set(),
            },
            "topic_constant": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: set(),
            },
            "user_interest": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: set(),
            },
            "user_list": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.AUTOMATIC_KEYS: {"customer_id"},
            },
            # Report objects

            # All reports have AUTOMATIC_KEYS that we include to delineate reporting data downstream.
            # These are fields that are inherently used by Google for each respective resource to aggregate metrics
            # shopping_performance_report's automatic_keys are currently unknown, and thus are temporarily empty

            "account_performance_report": { # accounts
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {"customer_id"},
            },
            "ad_group_audience_performance_report": {  # ad_group_audience_view
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "ad_group_criterion_criterion_id",
                    "ad_group_id",
                },
            },
            "ad_group_performance_report": {  # ad_group
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {"ad_group_id"},
            },
            "ad_performance_report": {  # ads
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {"id"},
            },
            "age_range_performance_report": {  # "age_range_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "ad_group_criterion_age_range",
                    "ad_group_criterion_criterion_id",
                    "ad_group_id",
                },
            },
            "campaign_audience_performance_report": {  # "campaign_audience_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "campaign_id",
                    "campaign_criterion_criterion_id",
                },
            },
            "campaign_performance_report": {  # "campaign"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {"campaign_id"}
            },

            "click_performance_report": { #  "click_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "clicks", # This metric is automatically included because it is the only metric available via the report
                    "click_view_gclid",
                },
            },
            "display_keyword_performance_report": {  # "display_keyword_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "ad_group_criterion_criterion_id",
                    "ad_group_id",
                },
            },
            "display_topics_performance_report": {  # "topic_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "ad_group_criterion_criterion_id",
                    "ad_group_id",
                },
            },
            "expanded_landing_page_report": {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {"expanded_landing_page_view_expanded_final_url"},
            },
            "gender_performance_report": {  # "gender_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "ad_group_criterion_criterion_id",
                    "ad_group_id",
                },
            },
            "geo_performance_report": {  # "geographic_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "geographic_view_country_criterion_id",
                    "geographic_view_location_type",
                }
            },
            "keywordless_query_report": {  # "dynamic_search_ads_search_term_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "ad_group_id",
                    "dynamic_search_ads_search_term_view_headline",
                    "dynamic_search_ads_search_term_view_landing_page",
                    "dynamic_search_ads_search_term_view_page_url",
                    "dynamic_search_ads_search_term_view_search_term",
                },
            },
            "keywords_performance_report": {  # "keyword_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "ad_group_criterion_criterion_id",
                    "ad_group_id",
                },
            },
            "landing_page_report": {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {"landing_page_view_unexpanded_final_url"},
            },
            "placement_performance_report": {  # "managed_placement_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "ad_group_criterion_criterion_id",
                    "ad_group_id",
                },
            },
            "search_query_performance_report": {  # "search_term_view"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {
                    "ad_group_id",
                    "campaign_id",
                    "search_term_view_search_term",
                },
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
                self.AUTOMATIC_KEYS: {
                    "user_location_view_country_criterion_id",
                    "user_location_view_targeting_location",
                },
            },
            "video_performance_report": {  # "video"
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date"},
                self.AUTOMATIC_KEYS: {"video_id"},
            },

            # Custom Reports TODO Post Beta feature
        }

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_automatic_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of automatic key fields
        """
        return {table: properties.get(self.AUTOMATIC_KEYS, set())
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
        """
        return a dictionary with key of table name
        and value as a set of all inclusion == automatic fields
        """
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) | \
                v.get(self.AUTOMATIC_KEYS, set())

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
                'click_view_ad_group_ad',
                'ad_group_id',
                'ad_group_name',
                'ad_group_status',
                'ad_network_type',
                'click_view_area_of_interest',
                'click_view_campaign_location_target',
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
                'placeholder_type',
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
