from collections import defaultdict
import json

import singer
from singer import Transformer

from google.protobuf.json_format import MessageToJson

from . import report_definitions

LOGGER = singer.get_logger()

API_VERSION = "v9"


def flatten(obj):
    """Given an `obj` like

    {"a" : {"b" : "c"},
     "d": "e"}

    return

    {"a.b": "c",
     "d": "e"}
    """
    new_obj = {}
    for key, value in obj.items():
        if isinstance(value, dict):
            for sub_key, sub_value in flatten(value).items():
                new_obj[f"{key}.{sub_key}"] = sub_value
        else:
            new_obj[key] = value
    return new_obj


class BaseStream:
    def sync(self, sdk_client, customer, stream):
        gas = sdk_client.get_service("GoogleAdsService", version=API_VERSION)
        resource_name = self.google_ads_resources_name[0]
        stream_name = stream["stream"]
        stream_mdata = stream["metadata"]
        selected_fields = []
        for mdata in stream_mdata:
            if (
                mdata["breadcrumb"]
                and mdata["metadata"].get("selected")
                and mdata["metadata"].get("inclusion") == "available"
            ):
                selected_fields.append(mdata["breadcrumb"][1])

        query = f"SELECT {','.join(selected_fields)} FROM {resource_name}"

        response = gas.search(query=query, customer_id=customer["customerId"])
        with Transformer() as transformer:
            json_response = [
                json.loads(MessageToJson(x, preserving_proto_field_name=True))
                for x in response
            ]
            for obj in json_response:
                flattened_obj = flatten(obj)
                record = transformer.transform(flattened_obj, stream["schema"])
                singer.write_record(stream_name, record)

    def add_extra_fields(self, resource_schema):
        """This function should add fields to `field_exclusions`, `schema`, and
        `behavior` that are not covered by Google's resource_schema
        """

    def extract_field_information(self, resource_schema):
        self.field_exclusions = defaultdict(set)
        self.schema = {}
        self.behavior = {}
        self.selectable = {}

        for resource_name in self.google_ads_resources_name:

            # field_exclusions step
            fields = resource_schema[resource_name]["fields"]
            for field_name, field in fields.items():
                if field_name in self.fields:
                    self.field_exclusions[field_name].update(
                        field["incompatible_fields"]
                    )

                    self.schema[field_name] = field["field_details"]["json_schema"]

                    self.behavior[field_name] = field["field_details"]["category"]

                    self.selectable[field_name] = field["field_details"]["selectable"]
            self.add_extra_fields(resource_schema)
        self.field_exclusions = {k: list(v) for k, v in self.field_exclusions.items()}

    def __init__(self, fields, google_ads_resource_name, resource_schema, primary_keys):
        self.fields = fields
        self.google_ads_resources_name = google_ads_resource_name
        self.primary_keys = primary_keys
        self.extract_field_information(resource_schema)


class AdGroupPerformanceReport(BaseStream):
    def add_extra_fields(self, resource_schema):
        # from the resource ad_group_ad_label
        field_name = "label.resource_name"
        # for field_name in []:
        self.field_exclusions[field_name] = {}
        self.schema[field_name] = {"type": ["null", "string"]}
        self.behavior[field_name] = "ATTRIBUTE"


class AdPerformanceReport(BaseStream):
    def add_extra_fields(self, resource_schema):
        # from the resource ad_group_ad_label
        for field_name in ["label.resource_name", "label.name"]:
            self.field_exclusions[field_name] = {}
            self.schema[field_name] = {"type": ["null", "string"]}
            self.behavior[field_name] = "ATTRIBUTE"

        for field_name in [
            "ad_group_criterion.negative",
        ]:
            self.field_exclusions[field_name] = {}
            self.schema[field_name] = {"type": ["null", "boolean"]}
            self.behavior[field_name] = "ATTRIBUTE"


class AudiencePerformanceReport(BaseStream):
    "hi"
    # COMMENT FROM GOOGLE
    #'bidding_strategy.name must be selected withy the resources  bidding_strategy or campaign.',

    # We think this means
    # `SELECT bidding_strategy.name from bidding_strategy`
    #  Not sure how this applies to the campaign resource

    # COMMENT FROM GOOGLE
    # 'campaign.bidding_strategy.type must be selected withy the resources bidding_strategy or campaign.'

    # We think this means
    # `SELECT bidding_strategy.type from bidding_strategy`

    # `SELECT campaign.bidding_strategy_type from campaign`

    # 'user_list.name' is a "Segmenting resource"
    # `select user_list.name from `

class CampaignPerformanceReport(BaseStream):
    # TODO: The sync needs to select from campaign_criterion if campaign_criterion.device.type is selected
    # TODO: The sync needs to select from campaign_label if label.resource_name
    def add_extra_fields(self, resource_schema):
        for field_name in [
            "campaign_criterion.device.type",
            "label.resource_name",
        ]:
            self.field_exclusions[field_name] = set()
            self.schema[field_name] = {"type": ["null", "string"]}
            self.behavior[field_name] = "ATTRIBUTE"


class DisplayKeywordPerformanceReport(BaseStream):
    # TODO: The sync needs to select from bidding_strategy and/or campaign if bidding_strategy.name is selected
    def add_extra_fields(self, resource_schema):
        for field_name in [
            "bidding_strategy.name",
        ]:
            self.field_exclusions[field_name] = resource_schema[
                self.google_ads_resources_name[0]
            ]["fields"][field_name]["incompatible_fields"]
            self.schema[field_name] = {"type": ["null", "string"]}
            self.behavior[field_name] = "SEGMENT"


class GeoPerformanceReport(BaseStream):
    # TODO: The sync needs to select from bidding_strategy and/or campaign if bidding_strategy.name is selected
    def add_extra_fields(self, resource_schema):
        for resource_name in self.google_ads_resources_name:
            for field_name in [
                "country_criterion_id",
            ]:
                full_field_name = f"{resource_name}.{field_name}"
                self.field_exclusions[full_field_name] = (
                    resource_schema[resource_name]["fields"][full_field_name][
                        "incompatible_fields"
                    ]
                    or set()
                )
                self.schema[full_field_name] = {"type": ["null", "string"]}
                self.behavior[full_field_name] = "ATTRIBUTE"


class KeywordsPerformanceReport(BaseStream):
    # TODO: The sync needs to select from ad_group_label if label.name is selected
    # TODO: The sync needs to select from ad_group_label if label.resource_name is selected
    def add_extra_fields(self, resource_schema):
        for field_name in [
            "label.resource_name",
            "label.name",
        ]:
            self.field_exclusions[field_name] = set()
            self.schema[field_name] = {"type": ["null", "string"]}
            self.behavior[field_name] = "ATTRIBUTE"


class PlaceholderFeedItemReport(BaseStream):
    # TODO: The sync needs to select from feed_item_target if feed_item_target.device is selected
    # TODO: The sync needs to select from feed_item if feed_item.policy_infos is selected
    def add_extra_fields(self, resource_schema):
        for field_name in ["feed_item_target.device", "feed_item.policy_infos"]:
            self.field_exclusions[field_name] = set()
            self.schema[field_name] = {"type": ["null", "string"]}
            self.behavior[field_name] = "ATTRIBUTE"


def initialize_core_streams(resource_schema):
    return {
        "Accounts": BaseStream(
            report_definitions.ACCOUNT_FIELDS,
            ["customer"],
            resource_schema,
            ["customer.id"],
        ),
        "Ad_Groups": BaseStream(
            report_definitions.AD_GROUP_FIELDS,
            ["ad_group"],
            resource_schema,
            ["ad_group.id"],
        ),
        "Ads": BaseStream(
            report_definitions.AD_GROUP_AD_FIELDS,
            ["ad_group_ad"],
            resource_schema,
            ["ad_group_ad.ad.id"],
        ),
        "Campaigns": BaseStream(
            report_definitions.CAMPAIGN_FIELDS,
            ["campaign"],
            resource_schema,
            ["campaign.id"],
        ),
    }


def initialize_reports(resource_schema):
    return {
        "Account_Performance_Report": BaseStream(
            report_definitions.ACCOUNT_PERFORMANCE_REPORT_FIELDS,
            ["customer"],
            resource_schema,
            ["customer.id"],
        ),
        # TODO: This needs to link with ad_group_ad_label
        "Adgroup_Performance_Report": AdGroupPerformanceReport(
            report_definitions.ADGROUP_PERFORMANCE_REPORT_FIELDS,
            ["ad_group"],
            resource_schema,
            ["ad_group.id"],
        ),
        "Ad_Performance_Report": AdPerformanceReport(
            report_definitions.AD_PERFORMANCE_REPORT_FIELDS,
            ["ad_group_ad"],
            resource_schema,
            ["ad_group_ad.ad.id"],
        ),
        "Age_Range_Performance_Report": BaseStream(
            report_definitions.AGE_RANGE_PERFORMANCE_REPORT_FIELDS,
            ["age_range_view"],
            resource_schema,
            ["ad_group_criterion.criterion_id"],
        ),
        "Audience_Performance_Report": AudiencePerformanceReport(
            report_definitions.AUDIENCE_PERFORMANCE_REPORT_FIELDS,
            ["campaign_audience_view", "ad_group_audience_view"],
            resource_schema,
            ["ad_group_criterion.criterion_id"],
        ),
        # "AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT": BaseStream(report_definitions.AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT_FIELDS, ["group_placement_view"], resource_schema),
        # "BID_GOAL_PERFORMANCE_REPORT": BaseStream(report_definitions.BID_GOAL_PERFORMANCE_REPORT_FIELDS, ["bidding_strategy"], resource_schema),
        # "BUDGET_PERFORMANCE_REPORT": BaseStream(report_definitions.BUDGET_PERFORMANCE_REPORT_FIELDS, ["campaign_budget"], resource_schema),
        "Call_Metrics_Call_Details_Report": BaseStream(
            report_definitions.CALL_METRICS_CALL_DETAILS_REPORT_FIELDS,
            ["call_view"],
            resource_schema,
            [""],
        ),
        # "CAMPAIGN_AD_SCHEDULE_TARGET_REPORT": BaseStream(report_definitions.CAMPAIGN_AD_SCHEDULE_TARGET_REPORT_FIELDS, ["ad_schedule_view"], resource_schema),
        # "CAMPAIGN_CRITERIA_REPORT": BaseStream(report_definitions.CAMPAIGN_CRITERIA_REPORT_FIELDS, ["campaign_criterion"], resource_schema),
        # "CAMPAIGN_LOCATION_TARGET_REPORT": BaseStream(report_definitions.CAMPAIGN_LOCATION_TARGET_REPORT_FIELDS, ["location_view"], resource_schema),
        "Campaign_Performance_Report": CampaignPerformanceReport(
            report_definitions.CAMPAIGN_PERFORMANCE_REPORT_FIELDS,
            ["campaign"],
            resource_schema,
            [""],
        ),
        # "CAMPAIGN_SHARED_SET_REPORT": BaseStream(report_definitions.CAMPAIGN_SHARED_SET_REPORT_FIELDS, ["campaign_shared_set"], resource_schema),
        "Click_Performance_Report": BaseStream(
            report_definitions.CLICK_PERFORMANCE_REPORT_FIELDS,
            ["click_view"],
            resource_schema,
            [""],
        ),
        "Display_Keyword_Performance_Report": DisplayKeywordPerformanceReport(
            report_definitions.DISPLAY_KEYWORD_PERFORMANCE_REPORT_FIELDS,
            ["display_keyword_view"],
            resource_schema,
            ["ad_group_criterion.criterion_id"],
        ),
        "Display_Topics_Performance_Report": DisplayKeywordPerformanceReport(
            report_definitions.DISPLAY_TOPICS_PERFORMANCE_REPORT_FIELDS,
            ["topic_view"],
            resource_schema,
            [""],
        ),
        "Gender_Performance_Report": BaseStream(
            report_definitions.GENDER_PERFORMANCE_REPORT_FIELDS,
            ["gender_view"],
            resource_schema,
            [""],
        ),
        "Geo_Performance_Report": GeoPerformanceReport(
            report_definitions.GEO_PERFORMANCE_REPORT_FIELDS,
            ["geographic_view", "user_location_view"],
            resource_schema,
            [""],
        ),
        "Keywordless_Query_Report": BaseStream(
            report_definitions.KEYWORDLESS_QUERY_REPORT_FIELDS,
            ["dynamic_search_ads_search_term_view"],
            resource_schema,
            [""],
        ),
        "Keywords_Performance_Report": KeywordsPerformanceReport(
            report_definitions.KEYWORDS_PERFORMANCE_REPORT_FIELDS,
            ["keyword_view"],
            resource_schema,
            [""],
        ),
        # "LABEL_REPORT": BaseStream(report_definitions.LABEL_REPORT_FIELDS, ["label"], resource_schema),
        # "LANDING_PAGE_REPORT": BaseStream(report_definitions.LANDING_PAGE_REPORT_FIELDS, ["landing_page_view", "expanded_landing_page_view"], resource_schema),
        # "PAID_ORGANIC_QUERY_REPORT": BaseStream(report_definitions.PAID_ORGANIC_QUERY_REPORT_FIELDS, ["paid_organic_search_term_view"], resource_schema),
        # "PARENTAL_STATUS_PERFORMANCE_REPORT": BaseStream(report_definitions.PARENTAL_STATUS_PERFORMANCE_REPORT_FIELDS, ["parental_status_view"], resource_schema),
        "Placeholder_Feed_Item_Report": PlaceholderFeedItemReport(
            report_definitions.PLACEHOLDER_FEED_ITEM_REPORT_FIELDS,
            ["feed_item", "feed_item_target"],
            resource_schema,
            [""],
        ),
        "Placeholder_Report": BaseStream(
            report_definitions.PLACEHOLDER_REPORT_FIELDS,
            ["feed_placeholder_view"],
            resource_schema,
            [""],
        ),
        "Placement_Performance_Report": BaseStream(
            report_definitions.PLACEMENT_PERFORMANCE_REPORT_FIELDS,
            ["managed_placement_view"],
            resource_schema,
            [""],
        ),
        # "PRODUCT_PARTITION_REPORT": BaseStream(report_definitions.PRODUCT_PARTITION_REPORT_FIELDS, ["product_group_view"], resource_schema),
        "Search_Query_Performance_Report": BaseStream(
            report_definitions.SEARCH_QUERY_PERFORMANCE_REPORT_FIELDS,
            ["search_term_view"],
            resource_schema,
            [""],
        ),
        # "SHARED_SET_CRITERIA_REPORT": BaseStream(report_definitions.SHARED_SET_CRITERIA_REPORT_FIELDS, ["shared_criterion"], resource_schema),
        "Shopping_Performance_Report": BaseStream(
            report_definitions.SHOPPING_PERFORMANCE_REPORT_FIELDS,
            ["shopping_performance_view"],
            resource_schema,
            [""],
        ),
        # "URL_PERFORMANCE_REPORT": BaseStream(report_definitions.URL_PERFORMANCE_REPORT_FIELDS, ["detail_placement_view"], resource_schema),
        # "USER_AD_DISTANCE_REPORT": BaseStream(report_definitions.USER_AD_DISTANCE_REPORT_FIELDS, ["distance_view"], resource_schema),
        "Video_Performance_Report": BaseStream(
            report_definitions.VIDEO_PERFORMANCE_REPORT_FIELDS,
            ["video"],
            resource_schema,
            [""],
        ),
        # RESOURCES: "",
    }
