"""Test tap discovery mode and metadata."""
import re

from tap_tester import menagerie, connections, runner

from base import GoogleAdsBase


class DiscoveryTest(GoogleAdsBase):
    """Test tap discovery mode and metadata conforms to standards."""

    def expected_fields(self):
        """The expected streams and metadata about the streams"""
        # TODO verify accounts, ads, ad_groups, campaigns contain foreign keys for
        #                  'campaign_budgets', 'bidding_strategies', 'accessible_bidding_strategies'
        #      and only foreign keys BUT CHECK DOCS

        return {
            # Core Objects
            "accounts": {  # TODO check with Brian on changes
                # OLD FIELDS (with mapping)
                "currency_code",
                "id", # "customer_id",
                "manager", # "can_manage_clients",
                "resource_name", # name -- unclear if this actually the mapping
                "test_account",
                "time_zone",  #"date_time_zone",
                # NEW FIELDS
                'auto_tagging_enabled',
                'call_reporting_setting.call_conversion_action',
                'call_reporting_setting.call_conversion_reporting_enabled',
                'call_reporting_setting.call_reporting_enabled',
                'conversion_tracking_setting.conversion_tracking_id',
                'conversion_tracking_setting.cross_account_conversion_tracking_id',
                'descriptive_name',
                'final_url_suffix',
                'has_partners_badge',
                'optimization_score',
                'optimization_score_weight',
                'pay_per_conversion_eligibility_failure_reasons',
                'remarketing_setting.google_global_site_tag',
                'tracking_url_template',
            },
            "campaigns": {  # TODO check out nested keys once these are satisfied
                # OLD FIELDS
                "ad_serving_optimization_status",
                "advertising_channel_type",
                "base_campaign", # Was "base_campaign_id",
                "campaign_budget_id", # Was "budget_id",
                "end_date",
                "experiment_type", # Was campaign_trial_type",
                "frequency_caps", # Was frequency_cap",
                "id",
                "labels",
                "name",
                "network_settings.target_content_network", # Was network_setting
                "network_settings.target_google_search", # Was network_setting
                "network_settings.target_partner_search_network", # Was network_setting
                "network_settings.target_search_network", # Was network_setting
                "serving_status",
                "start_date",
                "status",
                "url_custom_parameters",
                #"conversion_optimizer_eligibility", # No longer present
                #"settings", # No clear mapping to replacement
                # NEW FIELDS
                "accessible_bidding_strategy",
                "accessible_bidding_strategy_id",
                "advertising_channel_sub_type",
                "app_campaign_setting.app_id",
                "app_campaign_setting.app_store",
                "app_campaign_setting.bidding_strategy_goal_type",
                "bidding_strategy",
                "bidding_strategy_id",
                "bidding_strategy_type",
                "campaign_budget",
                "commission.commission_rate_micros",
                "customer_id",
                "dynamic_search_ads_setting.domain_name",
                "dynamic_search_ads_setting.feeds",
                "dynamic_search_ads_setting.language_code",
                "dynamic_search_ads_setting.use_supplied_urls_only",
                "excluded_parent_asset_field_types",
                "final_url_suffix",
                "geo_target_type_setting.negative_geo_target_type",
                "geo_target_type_setting.positive_geo_target_type",
                "hotel_setting.hotel_center_id",
                "local_campaign_setting.location_source_type",
                "manual_cpc.enhanced_cpc_enabled",
                "manual_cpm",
                "manual_cpv",
                "maximize_conversion_value.target_roas",
                "maximize_conversions.target_cpa",
                "optimization_goal_setting.optimization_goal_types",
                "optimization_score",
                "payment_mode",
                "percent_cpc.cpc_bid_ceiling_micros",
                "percent_cpc.enhanced_cpc_enabled",
                "real_time_bidding_setting.opt_in",
                "resource_name",
                "selective_optimization.conversion_actions",
                "shopping_setting.campaign_priority",
                "shopping_setting.campaign_priority",
                "shopping_setting.enable_local",
                "shopping_setting.merchant_id",
                "shopping_setting.sales_country",
                "target_cpa.cpc_bid_ceiling_micros",
                "target_cpa.cpc_bid_floor_micros",
                "target_cpa.target_cpa_micros",
                "target_cpm",
                "target_impression_share.cpc_bid_ceiling_micros",
                "target_impression_share.location",
                "target_impression_share.location_fraction_micros",
                "target_roas.cpc_bid_ceiling_micros",
                "target_roas.cpc_bid_floor_micros",
                "target_roas.target_roas",
                "target_spend.cpc_bid_ceiling_micros",
                "target_spend.target_spend_micros",
                "targeting_setting.target_restrictions",
                "tracking_setting.tracking_url",
                "tracking_url_template",
                "url_expansion_opt_out",
                "vanity_pharma.vanity_pharma_display_url_mode",
                "vanity_pharma.vanity_pharma_text",
                "video_brand_safety_suitability",
            },
            "ad_groups": {  # TODO check out nested keys once these are satisfied
                # OLD FIELDS (with mappings)
                "type",  # ("ad_group_type")
                "base_ad_group",  # ("base_ad_group_id")
                # "bidding_strategy_configuration", # DNE
                "campaign",  #("campaign_name", "campaign_id", "base_campaign_id") # TODO redo this
                "id",
                "labels",
                "name",
                # "settings", # DNE
                "status",
                "url_custom_parameters",
                # NEW FIELDS
                'resource_name',
                "tracking_url_template",
                "cpv_bid_micros",
                "campaign_id",
                "effective_target_cpa_micros",
                "display_custom_bid_dimension",
                "bidding_strategy_id",
                "target_cpm_micros",
                "explorer_auto_optimizer_setting.opt_in",
                "effective_target_cpa_source",
                "accessible_bidding_strategy_id",
                "excluded_parent_asset_field_types",
                "final_url_suffix",
                "percent_cpc_bid_micros",
                "effective_target_roas_source",
                "ad_rotation_mode",
                "targeting_setting.target_restrictions",
                "cpm_bid_micros",
                "customer_id",
                "cpc_bid_micros",
                "target_roas",
                "target_cpa_micros",
                "effective_target_roas",
            },
            "ads": {  # TODO check out nested keys once these are satisfied
                # OLD FIELDS (with mappings)
                "ad_group_id",
                "base_ad_group_id",
                "base_campaign_id",
                'policy_summary.policy_topic_entries',  # ("policy_summary")
                'policy_summary.review_status',  # ("policy_summary")
                'policy_summary.approval_status',  # ("policy_summary")
                "status",
                # "trademark_disapproved",  # DNE
                # NEW FIELDS
            },
            'campaign_budgets': {
                "budget_id",
            },
            'bidding_strategies': {
                "bids", # comparablevalue.type, microamount,
                "bid_source",
                "bids.type",
            },
            'accessible_bidding_strategies': {
                "bids", # comparablevalue.type, microamount,
                "bid_source",
                "bids.type",
            },
            # Report objects
            "age_range_performance_report": {  # "age_range_view"
            },
            "audience_performance_report": {  # "campaign_audience_view"
            },
            "campaign_performance_report": {  # "campaign_audience_view"
            },
            "call_metrics_call_details_report": {  # "call_view"
            },
            "click_performance_report": { #  "click_view"
            },
            "display_keyword_performance_report": {  # "display_keyword_view"
            },
            "display_topics_performance_report":{  # "topic_view"
            },
            "": {  # "topic_view" todo consult https://developers.google.com/google-ads/api/docs/migration/url-reports for migrating this report
            },
            "gender_performance_report": {  # "gender_view"
            },
            "geo_performance_report": {  # "geographic_view", "user_location_view"
            },
            "keywordless_query_report": {  # "dynamic_search_ads_search_term_view"
            },
            "keywords_performance_report": {  # "keyword_view"
            },
            "landing_page_view": {  # was final_url_report
            },
            "expanded_landing_page_view": {  # was final_url_report
            },
            "placeholder_feed_item_report": {  # "feed_item",  "feed_item_target"
            },
            "placeholder_report": { # "feed_placeholder_view"
            },
            "placement_performance_report": {  # "managed_placement_view"
            },
            "search_query_performance_report": {  # "search_term_view"
            },
            "shopping_performance_report": {  # "shopping_performance_view"
            },
            "video_performance_report": {  # "video"
            },
            "account_performance_report": { # accounts
            },
            "adgroup_performance_report": {  # ad_group
            },
            "ad_performance_report": {  # ads
            },
            # Custom Reports TODO feature
        }

    @staticmethod
    def name():
        return "tt_google_ads_disco"

    def test_run(self):
        """
        Testing that discovery creates the appropriate catalog with valid metadata.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys
          are given the inclusion of automatic.
        • verify that all other fields have inclusion of available metadata.
        """
        print("Discovery Test for tap-google-ads")

        conn_id = connections.ensure_connection(self)

        streams_to_test = self.expected_streams() - {
            # BUG_2 | missing
            'landing_page_report',
            'expanded_landing_page_report',
        }

        # found_catalogs = self.run_and_verify_check_mode(conn_id) # TODO PUT BACK
        # TODO REMOVE FROM HERE
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0)
        found_catalog_names = {found_catalog['stream_name'] for found_catalog in found_catalogs}
        self.assertSetEqual(streams_to_test, found_catalog_names)
        # TODO TO HERE

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
                          msg="One or more streams don't follow standard naming")

        for stream in streams_to_test: # {'accounts', 'campaigns', 'ad_groups', 'ads'}: # # TODO PUT BACK
            with self.subTest(stream=stream):

                # Verify the catalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_foreign_keys = self.expected_foreign_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = expected_primary_keys | expected_replication_keys | expected_foreign_keys
                expected_replication_method = self.expected_replication_method()[stream]
                expected_fields = self.expected_fields()[stream]

                # collecting actual values
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_primary_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])
                )
                actual_foreign_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.FOREIGN_KEYS: []}).get(self.FOREIGN_KEYS, [])
                )
                actual_replication_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])
                )
                actual_replication_method = stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in metadata
 if item.get("metadata").get("inclusion") == "automatic"
                )
                actual_fields = []
                for md_entry in metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                ##########################################################################
                ### metadata assertions
                ##########################################################################

                # verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # verify there are no duplicate metadata entries
                self.assertEqual(len(actual_fields), len(set(actual_fields)), msg="duplicates in the fields retrieved")


                # verify the tap_stream_id and stream_name are consistent (only applies to SaaS taps)
                if self.is_report(stream): # BUG_TODO not true for core streams (unclear on significance in saas tap ?) 
                    self.assertEqual(catalog['stream_name'], catalog['tap_stream_id'])

                # verify primary key(s)
                if not self.is_report(stream): # BUG_TODO primary keys md missing for reports
                    self.assertSetEqual(expected_primary_keys, actual_primary_keys)

                # BUG_TODO | all core and report streams are missing this metadata
                # verify replication method
                # self.assertEqual(expected_replication_method, actual_replication_method)

                # BUG_TODO_1 md missing for report streams expected 'date' key
                # verify replication key(s)
                # self.assertSetEqual(expected_replication_keys, actual_replication_keys)

                # verify foreign keys are present for each stream (core streams only)
                self.assertSetEqual(expected_foreign_keys, actual_foreign_keys)

                # verify foreign keys are given inclusion of automatic TODO

                # verify replication key is present for any stream with replication method = INCREMENTAL
                if actual_replication_method == 'INCREMENTAL':
                    # BUG_TODO_1 | Implement when md present
                    # self.assertEqual(expected_replication_keys, actual_replication_keys)
                    pass
                else:
                    self.assertEqual(actual_replication_keys, set())

                # verify all expected fields are found # TODO set expectations
                # self.assertSetEqual(expected_fields, set(actual_fields))

                # verify the stream is given the inclusion of available
                self.assertEqual(catalog['metadata']['inclusion'], 'available', msg=f"{stream} cannot be selected")

                # BUG_TODO no streams have any of the expected fields with this inclusion value
                # verify the primary, replication keys are given the inclusions of automatic
                # self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                # verify all other fields are given inclusion of available
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") in {"available"}
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in metadata")

                # verify field exclusions for each strema match our expectations
                # TODO further tests may be needed, including attempted syncs with invalid field combos
