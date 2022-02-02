"""Test tap discovery mode and metadata."""
import re

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class DiscoveryTest(GoogleAdsBase):
    """
    Test tap's sync mode can extract records for all streams
    with standard table and field selection.
    """

    @staticmethod
    def expected_default_fields():
        """
        In this test core streams have all fields selected.

        Report streams will select fields based on the default values that
        are provided when selecting the report type in Google's UI.

        returns a dictionary of reports to standard fields
        """

        # BUG_TODO_1 commented out fields below are not discovered for the given stream by the tap
        return {
            # "account_performance_report": set(),
            'ad_performance_report': {
                # 'account_name', # 'Account name',
                # 'ad_final_url',  # 'Ad final URL',
                # 'ad_group',  # 'Ad group',
                # 'ad_mobile_final_url',  # 'Ad mobile final URL',
                'average_cpc',  # 'Avg. CPC',
                # 'business_name',  # 'Business name',
                # 'call_to_action_text',  # 'Call to action text',
                # 'campaign',  # 'Campaign',
                # 'campaign_subtype',  # 'Campaign type',
                # 'campaign_type',  # 'Campaign subtype',
                'clicks',  # 'Clicks',
                'conversions',  # 'Conversions',
                # 'conversion_rate',  # 'Conv. rate',
                # 'cost',  # 'Cost',
                'cost_per_conversion',  # 'Cost / conv.',
                'ctr',  # 'CTR',
                # 'currency_code',  # 'Currency code',
                'customer_id',  # 'Customer ID',
                # 'description',  # 'Description',
                # 'description_1',  # 'Description 1',
                # 'description_2',  # 'Description 2',
                # 'description_3',  # 'Description 3',
                # 'description_4',  # 'Description 4',
                # 'final_url',  # 'Final URL',
                # 'headline_1',  # 'Headline 1',
                # 'headline_2',  # 'Headline 2',
                # 'headline_3',  # 'Headline 3',
                # 'headline_4',  # 'Headline 4',
                # 'headline_5',  # 'Headline 5',
                'impressions',  # 'Impr.',
                # 'long_headline',  # 'Long headline',
                'view_through_conversions',  # 'View-through conv.',
            },
            "adgroup_performance_report": {
                # 'account_name',  # Account name,
                # 'ad_group',  # Ad group,
                # 'ad_group_state',  # Ad group state,
                'average_cpc',  # Avg. CPC,
                # 'campaign',  # Campaign,
                # 'campaign_subtype',  # Campaign subtype,
                # 'campaign_type',  # Campaign type,
                'clicks',  # Clicks,
                # 'conversion_rate',  # Conv. rate
                'conversions',  # Conversions,
                # 'cost',  # Cost,
                'cost_per_conversion',  # Cost / conv.,
                'ctr',  # CTR,
                # 'currency_code',  # Currency code,
                'customer_id',  # Customer ID,
                'impressions',  # Impr.,
                'view_through_conversions',  # View-through conv.,
            },
            # "age_range_performance_report": set(),
            "audience_performance_report": {
                # 'account_name', # Account name,
                # 'ad_group',  # Ad group,
                # 'ad_group_default_max_cpc',  # Ad group default max. CPC,
                # 'audience_segment',  # Audience segment,
                # 'audience_segment_bid_adjustments',  # Audience Segment Bid adj.,
                # 'audience_segment_max_cpc',  # Audience segment max CPC,
                # 'audience_segment_state',  # Audience segment state,
                'average_cpc',  # Avg. CPC,
                'average_cpm',  # Avg. CPM
                # 'campaign',  # Campaign,
                'clicks',  # Clicks,
                # 'cost',  # Cost,
                'ctr',  # CTR,
                # 'currency_code',  # Currency code,
                'customer_id',  # Customer ID,
                'impressions',  # Impr.,
                # 'targeting_setting',  # Targeting Setting,
            },
            "campaign_performance_report": {
                # 'account_name',  # Account name,
                'average_cpc',  # Avg. CPC,
                # 'campaign',  # Campaign,
                # 'campaign_state',  # Campaign state,
                # 'campaign_type',  # Campaign type,
                'clicks',  # Clicks,
                # 'conversion_rate',  # Conv. rate
                'conversions',  # Conversions,
                # 'cost',  # Cost,
                'cost_per_conversion',  # Cost / conv.,
                'ctr',  # CTR,
                # 'currency_code',  # Currency code,
                'customer_id',  # Customer ID,
                'impressions',  # Impr.,
                'view_through_conversions',  # View-through conv.,
            },
            # "click_performance_report": set(),
            # "display_keyword_performance_report": set(),
            # "display_topics_performance_report": set(),
            # "gender_performance_report": set(),
            # "geo_performance_report": set(),
            # "keywords_performance_report": set(),
            # "placement_performance_report": set(),
            # "search_query_performance_report": set(),
            # "shopping_performance_report": set(),
            # "video_performance_report": set(),
        }
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
    def name():
        return "tt_google_ads_canary"

    def test_run(self):
        """
        Testing that basic sync functions without Critical Errors
        """
        print("Discovery Test for tap-google-ads")

        conn_id = connections.ensure_connection(self)

        streams_to_test = self.expected_streams() - {
            # TODO we are only testing core strems at the moment
            'landing_page_report',
            'expanded_landing_page_report',
        }

        # Run a discovery job
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify a catalog was produced for each stream under test
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0)
        found_catalog_names = {found_catalog['stream_name'] for found_catalog in found_catalogs}
        self.assertSetEqual(streams_to_test, found_catalog_names)

        # Perform table and field selection...
        core_catalogs = [catalog for catalog in found_catalogs if not self.is_report(catalog['stream_name'])]
        # select all fields for core streams and...
        self.select_all_streams_and_fields(conn_id, core_catalogs, select_all_fields=True)


        # [WIP] Attempting field selection for a report
        # select 'default' fields for report streams
        for report in self.expected_default_fields().keys():

            # BUG_TODO_2 REQUESTED_METRICS_FOR_MANAGER
            if report in {'ad_performance_report',
                          'audience_performance_report',
                          'campaign_performance_report'}:
                continue

            # BUG_TODO_2 keyerror on 'id'
            if report in {'adgroup_performance_report'}:
                continue

            catalog = [catalog for catalog in found_catalogs
                       if catalog['stream_name'] == report][0]
            schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            metadata = schema_and_metadata['metadata']
            properties = {md['breadcrumb'][-1]
                          for md in metadata
                          if len(md['breadcrumb']) > 0 and md['breadcrumb'][0] == 'properties'}
            # TODO Use this pattern for testing field selection based on 'behavior'
            # property_breakdowns = {'SEGMENT': set(), 'ATTRIBUTE': set(), 'METRIC': set(),}
            # for md in metadata:
            #     if md['breadcrumb'] != []:
            #         property_breakdowns[md['metadata']['behavior']].add(md['breadcrumb'][-1])
            expected_fields = self.expected_default_fields()[catalog['stream_name']]
            self.assertTrue(expected_fields.issubset(properties),
                            msg=f"{report} missing {expected_fields.difference(properties)}")
            non_selected_properties = properties.difference(expected_fields)
            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema_and_metadata, [], non_selected_properties
            )
        # [WIP] END


        # Run a sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # acquire records from target output
        synced_records = runner.get_records_from_target_output()

        # Verify at least 1 record was replicated for each stream
        for stream in streams_to_test:

            if self.is_report(stream) and stream not in self.expected_default_fields().keys():
                print(f"SKIPPING ASSERTIONS FOR {stream}.")
                continue # TODO remove when field selection for reports is figured out

            with self.subTest(stream=stream):
                record_count = len(synced_records.get(stream, {'messages': []})['messages'])
                self.assertGreater(record_count, 0)
                print(f"{stream} canary survived.")
