#!/usr/bin/env python3
import json
import os
import sys

import singer
from singer import utils
from singer import metrics
from singer import bookmarks
from singer import metadata
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer)

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf.json_format import MessageToJson

from collections import defaultdict

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "oauth_client_id",
    "oauth_client_secret",
    "refresh_token",
    "customer_ids",
    "developer_token",
]

CORE_ENDPOINT_MAPPINGS =    {"campaigns": {'primary_keys': ["id"],
                                           'service_name': 'CampaignService'},
                             "ad_groups": {'primary_keys': ["id"],
                                           'service_name': 'AdGroupService'},
                             "ads":       {'primary_keys': ["id"],
                                           'service_name': 'AdGroupAdService'},
                             "accounts":  {'primary_keys': ["id"],
                                           'service_name': 'ManagedCustomerService'}}

#### Start Field Exclusion Stuff

def do_field_exclusion(config):
    accounts = json.loads(config['login_customer_ids'])

    
    client = GoogleAdsClient.load_from_dict(
        get_client_config(config)
    )

    gaf_service = client.get_service("GoogleAdsFieldService")

    exclusions = defaultdict(lambda: defaultdict(list))
    
    # for report_type in ['customer', 'ad_group_ad']:
    for report_type in ["customer", "ad_group_ad", "ad_group", "age_range_view", "campaign_audience_view", "group_placement_view", "bidding_strategy", "campaign_budget", "call_view", "ad_schedule_view", "campaign_criterion", "campaign", "campaign_shared_set", "location_view", "click_view", "display_keyword_view", "topic_view", "gender_view", "geographic_view", "dynamic_search_ads_search_term_view", "keyword_view", "label", "landing_page_view", "paid_organic_search_term_view", "parental_status_view", "feed_item", "feed_placeholder_view", "managed_placement_view", "product_group_view", "search_term_view", "shared_criterion", "shared_set", "shopping_performance_view", "detail_placement_view", "distance_view", "video"]:

        report_response = gaf_service.get_google_ads_field({'resource_name': f'googleAdsFields/{report_type}'})

        metrics = [x for x in report_response.metrics]
        segments = [x for x in report_response.segments]
        fields = metrics + segments
        print(f'About to run for {len(fields)} fields')
        for field in fields:
            field_response = gaf_service.get_google_ads_field({'resource_name': f'googleAdsFields/{field}'})
            exclusions[report_type][field].extend([x for x in field_response.selectable_with])

    import ipdb; ipdb.set_trace()
    1+1

    

        


### End Field Exclusion Stuff


def create_field_metadata(stream, schema):
    primary_key = CORE_ENDPOINT_MAPPINGS[stream]['primary_keys']

    mdata = {}
    mdata = metadata.write(mdata, (), 'inclusion', 'available')
    mdata = metadata.write(mdata, (), 'table-key-properties', primary_key)

    for field in schema['properties']:
        breadcrumb = ('properties', str(field))
        mdata = metadata.write(mdata, breadcrumb, 'inclusion', 'available')

    mdata = metadata.write(mdata, ('properties', primary_key[0]), 'inclusion', 'automatic')
    mdata = metadata.to_list(mdata)

    return mdata

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path(f"schemas/{entity}.json"))

def load_metadata(entity):
    return utils.load_json(get_abs_path(f"metadata/{entity}.json"))

def do_discover_core_endpoints():
    streams = []
    LOGGER.info("Starting core discovery")
    for stream_name in CORE_ENDPOINT_MAPPINGS:
        LOGGER.info('Loading schema for %s', stream_name)
        schema = load_schema(stream_name)
        md = create_field_metadata(stream_name, schema)
        streams.append({'stream': stream_name,
                        'tap_stream_id': stream_name,
                        'schema': schema,
                        'metadata': md})
    return streams

def do_discover_reports(config, client):
    accounts = json.loads(config['login_customer_ids'])

    googleads_service = client.get_service("GoogleAdsService")

    query = ('SELECT customer_client.id '
             'FROM customer_client '
             'WHERE customer_client.level <= 1')
    for account in accounts:
        customer_id = account['customerId']
        login_customer_id = account['loginCustomerId']

        response = googleads_service.search(customer_id=customer_id,
                                            query=query)
        
        import ipdb; ipdb.set_trace()
        1+1
    return []

def do_discover_reports(config):
    accounts = json.loads(config['login_customer_ids'])

    query = """
    SELECT customer.currency_code,
customer.descriptive_name,
customer.time_zone,
metrics.active_view_cpm,
metrics.active_view_ctr,
metrics.active_view_impressions,
metrics.active_view_measurability,
metrics.active_view_measurable_cost_micros,
metrics.active_view_measurable_impressions,
metrics.active_view_viewability,
segments.ad_network_type,
segments.ad_network_type,
metrics.all_conversions_from_interactions_rate,
metrics.all_conversions_value,
metrics.all_conversions,
metrics.average_cost,
metrics.average_cpc,
metrics.average_cpe,
metrics.average_cpm,
metrics.average_cpv,
customer.manager,
segments.click_type,
metrics.clicks,
metrics.content_budget_lost_impression_share,
metrics.content_impression_share,
metrics.content_rank_lost_impression_share,
segments.conversion_adjustment,
segments.conversion_or_adjustment_lag_bucket,
segments.conversion_action_category,
segments.conversion_lag_bucket,
metrics.conversions_from_interactions_rate,
segments.conversion_action,
segments.conversion_action_name,
metrics.conversions_value,
metrics.conversions,
metrics.cost_micros,
metrics.cost_per_all_conversions,
metrics.cost_per_conversion,
metrics.cross_device_conversions,
metrics.ctr,
customer.descriptive_name,
segments.date,
segments.day_of_week,
segments.device,
metrics.engagement_rate,
metrics.engagements,
segments.external_conversion_source,
customer.id,
segments.hour,
metrics.impressions,
metrics.interaction_rate,
metrics.interaction_event_types,
metrics.interactions,
metrics.invalid_click_rate,
metrics.invalid_clicks,
customer.auto_tagging_enabled,
customer.test_account,
segments.month,
segments.month_of_year,
segments.quarter,
metrics.search_budget_lost_impression_share,
metrics.search_exact_match_impression_share,
metrics.search_impression_share,
metrics.search_rank_lost_impression_share,
segments.slot,
metrics.value_per_all_conversions,
metrics.value_per_conversion,
metrics.video_view_rate,
metrics.video_views,
metrics.view_through_conversions,
segments.week,
segments.year
    FROM customer
    WHERE segments.date DURING LAST_7_DAYS
    """

    customer_segments = ['segments.ad_network_type', 'segments.click_type', 'segments.conversion_action', 'segments.conversion_action_category', 'segments.conversion_action_name', 'segments.conversion_adjustment', 'segments.conversion_lag_bucket', 'segments.conversion_or_adjustment_lag_bucket', 'segments.conversion_value_rule_primary_dimension', 'segments.date', 'segments.day_of_week', 'segments.device', 'segments.external_conversion_source', 'segments.hour', 'segments.month', 'segments.month_of_year', 'segments.quarter', 'segments.recommendation_type', 'segments.sk_ad_network_conversion_value', 'segments.slot', 'segments.week', 'segments.year']

    customer_metrics = ['metrics.active_view_cpm', 'metrics.active_view_ctr', 'metrics.active_view_impressions', 'metrics.active_view_measurability', 'metrics.active_view_measurable_cost_micros', 'metrics.active_view_measurable_impressions', 'metrics.active_view_viewability', 'metrics.all_conversions', 'metrics.all_conversions_by_conversion_date', 'metrics.all_conversions_from_interactions_rate', 'metrics.all_conversions_value', 'metrics.all_conversions_value_by_conversion_date', 'metrics.average_cost', 'metrics.average_cpc', 'metrics.average_cpe', 'metrics.average_cpm', 'metrics.average_cpv', 'metrics.clicks', 'metrics.content_budget_lost_impression_share', 'metrics.content_impression_share', 'metrics.content_rank_lost_impression_share', 'metrics.conversions', 'metrics.conversions_by_conversion_date', 'metrics.conversions_from_interactions_rate', 'metrics.conversions_value', 'metrics.conversions_value_by_conversion_date', 'metrics.cost_micros', 'metrics.cost_per_all_conversions', 'metrics.cost_per_conversion', 'metrics.cross_device_conversions', 'metrics.ctr', 'metrics.engagement_rate', 'metrics.engagements', 'metrics.impressions', 'metrics.interaction_event_types', 'metrics.interaction_rate', 'metrics.interactions', 'metrics.invalid_click_rate', 'metrics.invalid_clicks', 'metrics.optimization_score_uplift', 'metrics.optimization_score_url', 'metrics.search_budget_lost_impression_share', 'metrics.search_exact_match_impression_share', 'metrics.search_impression_share', 'metrics.search_rank_lost_impression_share', 'metrics.sk_ad_network_conversions', 'metrics.value_per_all_conversions', 'metrics.value_per_all_conversions_by_conversion_date', 'metrics.value_per_conversion', 'metrics.value_per_conversions_by_conversion_date', 'metrics.video_view_rate', 'metrics.video_views', 'metrics.view_through_conversions']
    
    for account in accounts:
        customer_id = account['customerId']
        login_customer_id = account['loginCustomerId']
        
        client = GoogleAdsClient.load_from_dict(
            get_client_config(config, login_customer_id)
        )
        
        googleads_service = client.get_service("GoogleAdsService")

        gaf_service = client.get_service("GoogleAdsFieldService")

        try:
            # response = googleads_service.search(customer_id=customer_id,
            #                                     query=query)
            # import ipdb; ipdb.set_trace()
            # 1+1
            response = gaf_service.get_google_ads_field({'resource_name':'googleAdsFields/segments.click_type'})
            import ipdb; ipdb.set_trace()
            1+1
        except Exception as error:
            import ipdb; ipdb.set_trace()
            1+1

        for row in response:
            print(row)
        import ipdb; ipdb.set_trace()
        1+1
    return []

def do_discover(config):
    #sdk_client = create_sdk_client(config)
    #client_config = get_client_config(config)
    core_streams = do_discover_core_endpoints()
    # report_streams = do_discover_reports(config, sdk_client)
    report_streams = do_discover_reports(config)
    streams = []
    streams.extend(core_streams)
    streams.extend(report_streams)
    json.dump({"streams": streams}, sys.stdout, indent=2)

def create_sdk_client(config):
    CONFIG = {
        'use_proto_plus': False,
        'developer_token': config['developer_token'],
        'client_id': config['oauth_client_id'],
        'client_secret': config['oauth_client_secret'],
        #'access_token': config['access_token'],
        'refresh_token': config['refresh_token'],
    }
    sdk_client = GoogleAdsClient.load_from_dict(CONFIG)
    return sdk_client

def get_client_config(config, login_customer_id=None):

    if login_customer_id:
        return {
            'use_proto_plus': False,
            'developer_token': config['developer_token'],
            'client_id': config['oauth_client_id'],
            'client_secret': config['oauth_client_secret'],
            'refresh_token': config['refresh_token'],
            'login_customer_id': login_customer_id,
        }
    else:
        return {
            'use_proto_plus': False,
            'developer_token': config['developer_token'],
            'client_id': config['oauth_client_id'],
            'client_secret': config['oauth_client_secret'],
            'refresh_token': config['refresh_token'],
        }


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        #do_discover(args.config)
        do_field_exclusion(args.config)
        LOGGER.info("Discovery complete")

if __name__ == "__main__":
    main()
