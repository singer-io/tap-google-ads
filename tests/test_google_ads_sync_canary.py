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

        # TODO_TDL-17909 [BUG?] commented out fields below are not discovered for the given stream by the tap
        return {
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
            "ad_group_performance_report": {
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
            # TODO_TDL-17909 | [BUG?] missing audience fields
            "ad_group_audience_performance_report": {
                # 'account_name', # Account name,
                # 'ad_group_name', # 'ad_group',  # Ad group,
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
                'ad_group_targeting_setting',  # Targeting Setting,
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
            "click_performance_report": {
                'ad_group_ad',
                'ad_group_id',
                'ad_group_name',
                'ad_group_status',
                'ad_network_type',
                'area_of_interest',
                'campaign_location_target',
                'click_type',
                'clicks',
                'customer_descriptive_name',
                'customer_id',
                'device',
                'gclid',
                'location_of_presence',
                'month_of_year',
                'page_number',
                'slot',
                'user_list',
            },
            "display_keyword_performance_report": { # TODO_TDL-17885 NO DATA AVAILABLE
                # 'ad_group',  # Ad group,
                # 'ad_group_bid_strategy_type',  # Ad group bid strategy type,
                'average_cpc',  # Avg. CPC,
                'average_cpm',  # Avg. CPM,
                'average_cpv',  # Avg. CPV,
                # 'campaign',  # Campaign,
                # 'campaign_bid_strategy_type',  # Campaign bid strategy type,
                # 'campaign_subtype',  # Campaign subtype,
                'clicks',  # Clicks,
                # 'conversion_rate',  # Conv. rate,
                'conversions',  # Conversions,
                # 'cost',  # Cost,
                'cost_per_conversion',  # Cost / conv.,
                # 'currency_code',  # Currency code,
                # 'display_video_keyword',  # Display/video keyword,
                'impressions',  # Impr.,
                'interaction_rate',  # Interaction rate,
                'interactions',  # Interactions,
                'view_through_conversions',  # View-through conv.,
            },
            "display_topics_performance_report": { # TODO_TDL-17885 NO DATA AVAILABLE
                'ad_group_name', # 'ad_group',  # Ad group,
                'average_cpc',  # Avg. CPC,
                'average_cpm',  # Avg. CPM,
                'campaign_name',  # 'campaign',  # Campaign,
                'clicks',  # Clicks,
                # 'cost',  # Cost,
                'ctr',  # CTR,
                'customer_currency_code',  # 'currency_code',  # Currency code,
                'impressions',  # Impr.,
                # 'topic',  # Topic,
                # 'topic_state',  # Topic state,
            },
            "placement_performance_report": { # TODO_TDL-17885 NO DATA AVAILABLE
                # 'ad_group_name',
                # 'ad_group_id',
                # 'campaign_name',
                # 'campaign_id',
                'clicks',
                'impressions',  # Impr.,
                # 'cost',
                'ad_group_criterion_placement',  # 'placement_group', 'placement_type',
            },
            # "keywords_performance_report": set(),
            # "shopping_performance_report": set(),
            # BUG
            "video_performance_report": {
                # 'ad_group_name',
                # 'all_conversions',
                # 'all_conversions_from_interactions_rate',
                # 'all_conversions_value',
                # 'average_cpm',
                # 'average_cpv',
                'campaign_name',
                'clicks',
                # 'conversions',
                # 'conversions_value',
                # 'cost_per_all_conversions',
                # 'cost_per_conversion',
                # 'customer_descriptive_name',
                # 'customer_id',
                # 'impressions',
                'video_quartile_p25_rate',
                # 'view_through_conversions',
            },
            # NOTE AFTER THIS POINT COULDN"T FIND IN UI
            "account_performance_report": {
                'average_cpc',
                'click_type',
                'clicks',
                'date',
                'descriptive_name',
                'id',
                'impressions',
                'invalid_clicks',
                'manager',
                'test_account',
                'time_zone',
            },
            "geo_performance_report": {
                'clicks',
                'ctr',  # CTR,
                'impressions',  # Impr.,
                'average_cpc',
                # 'cost',
                'conversions',
                'view_through_conversions',  # View-through conv.,
                'cost_per_conversion',  # Cost / conv.,
                # 'conversion_rate',  # Conv. rate
                # 'geo_target_city',
                # 'geo_target_metro',
                # 'geo_target_most_specific_location',
                'geo_target_region',
                # 'country_criterion_id',  # TODO_TDL-17910 | [BUG?] PROHIBITED_RESOURCE_TYPE_IN_SELECT_CLAUSE
           },
            "gender_performance_report": {
                # 'account_name',  # Account name,
                # 'ad_group',  # Ad group,
                # 'ad_group_state',  # Ad group state,
                'ad_group_criterion_gender',
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
            "search_query_performance_report": {
                'clicks',
                'ctr',  # CTR,
                'impressions',  # Impr.,
                'average_cpc',
                # 'cost',
                'conversions',
                'view_through_conversions',  # View-through conv.,
                'cost_per_conversion',  # Cost / conv.,
                # 'conversion_rate',  # Conv. rate
                'search_term',
                'search_term_match_type',
            },
            "age_range_performance_report": {
                'clicks',
                'ctr',  # CTR,
                'impressions',  # Impr.,
                'average_cpc',
                # 'cost',
                'conversions',
                'view_through_conversions',  # View-through conv.,
                'cost_per_conversion',  # Cost / conv.,
                # 'conversion_rate',  # Conv. rate
                'ad_group_criterion_age_range', # 'Age',
            },
            'placeholder_feed_item_report': {
                'clicks',
                'impressions',
                'placeholder_type',
            },
            'placeholder_report': {
                # 'ad_group_id',
                # 'ad_group_name',
                # 'average_cpc',
                # 'click_type',
                'clicks',
                'cost_micros',
                # 'customer_descriptive_name',
                # 'customer_id',
                'interactions',
                'placeholder_type',
            },
            # 'landing_page_report': set(), # TODO_TDL-17885
            # 'expanded_landing_page_report': set(), # TODO_TDL-17885
        }

    @staticmethod
    def name():
        return "tt_google_ads_canary"

    def test_run(self):
        """
        Testing that basic sync functions without Critical Errors
        """
        print("Canary Sync Test for tap-google-ads")

        conn_id = connections.ensure_connection(self)

        streams_to_test = self.expected_streams() - {
            # TODO_TDL-17885 the following are not yet implemented
            'display_keyword_performance_report', # no test data available
            'display_topics_performance_report',  # no test data available
            'ad_group_audience_performance_report',  # Potential BUG see above
            'placement_performance_report',  # no test data available
            "keywords_performance_report",  # no test data available
            "keywordless_query_report",  # no test data available
            "shopping_performance_report",  # cannot find this in GoogleUI
            "video_performance_report",  # no test data available
            "user_location_performance_report",  # no test data available
            'landing_page_report',  # not attempted 
            'expanded_landing_page_report', # not attempted
        }

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Perform table and field selection...
        core_catalogs = [catalog for catalog in found_catalogs
                         if not self.is_report(catalog['stream_name'])
                         and catalog['stream_name'] in streams_to_test]
        report_catalogs = [catalog for catalog in found_catalogs
                           if self.is_report(catalog['stream_name'])
                           and catalog['stream_name'] in streams_to_test]
        # select all fields for core streams and...
        self.select_all_streams_and_fields(conn_id, core_catalogs, select_all_fields=True)
        # select 'default' fields for report streams
        self.select_all_streams_and_default_fields(conn_id, report_catalogs)

        # Run a sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # acquire records from target output
        synced_records = runner.get_records_from_target_output()

        # Verify at least 1 record was replicated for each stream
        for stream in streams_to_test:

            with self.subTest(stream=stream):
                record_count = len(synced_records.get(stream, {'messages': []})['messages'])
                self.assertGreater(record_count, 0)
                print(f"{record_count} {stream} record(s) replicated.")

