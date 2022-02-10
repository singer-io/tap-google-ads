from collections import defaultdict
import json
import hashlib
from datetime import timedelta
import singer
from singer import Transformer, utils
from google.protobuf.json_format import MessageToJson
from . import report_definitions

LOGGER = singer.get_logger()

API_VERSION = "v9"

CORE_STREAMS = [
    "customer",
    "ad_group",
    "ad_group_ad",
    "campaign",
    "bidding_strategy",
    "accessible_bidding_strategy",
    "campaign_budget",
]

DEFAULT_CONVERSION_WINDOW = 30
NUM_DAYS_TO_QUERY = 7


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


def make_field_names(resource_name, fields):
    transformed_fields = []
    for field in fields:
        pieces = field.split("_")
        front = "_".join(pieces[:-1])
        back = pieces[-1]

        if '.' in field:
            transformed_fields.append(f"{resource_name}.{field}")
        elif front in CORE_STREAMS and field.endswith('_id'):
            transformed_fields.append(f"{front}.{back}")
        else:
            transformed_fields.append(f"{resource_name}.{field}")
    return transformed_fields


def create_core_stream_query(resource_name, stream_mdata):
    selected_fields = set()
    for mdata in stream_mdata:
        if (
            mdata["breadcrumb"]
            and mdata["metadata"].get("selected")
            and (
                mdata["metadata"].get("inclusion") == "available"
                or mdata["metadata"].get("inclusion") == "automatic")
        ):
            selected_fields.update(mdata['metadata']["tap-google-ads.api-field-names"])

    return f"SELECT {','.join(selected_fields)} FROM {resource_name}"


def create_report_query(resource_name, stream_mdata, query_start_date, query_end_date):
    selected_fields = set()
    for mdata in stream_mdata:
        if (
            mdata["breadcrumb"]
            and mdata["metadata"].get("selected")
            and (
                mdata["metadata"].get("inclusion") == "available"
                or mdata["metadata"].get("inclusion") == "automatic")
            and mdata["breadcrumb"][1] != "_sdc_record_hash"
        ):
            selected_fields.update(mdata['metadata']["tap-google-ads.api-field-names"])

    format_str = '%Y-%m-%d'
    query_start_date = utils.strftime(query_start_date, format_str=format_str)
    query_end_date = utils.strftime(query_end_date, format_str=format_str)
    return f"SELECT {','.join(selected_fields)} FROM {resource_name} WHERE segments.date BETWEEN '{query_start_date}' AND '{query_end_date}'"


def generate_hash(record, metadata):
    metadata = singer.metadata.to_map(metadata)
    fields_to_hash = {}
    for key, val in record.items():
        if metadata[('properties', key)]['behavior'] != "METRIC":
            fields_to_hash[key] = val

    hash_source_data = {key: fields_to_hash[key] for key in sorted(fields_to_hash)}
    hash_bytes = json.dumps(hash_source_data).encode('utf-8')
    return hashlib.sha256(hash_bytes).hexdigest()


# Todo Create report stream class
class BaseStream:
    def transform_keys(self, obj):
        target_resource_name = self.google_ads_resources_name[0]
        transformed_obj = {}

        for resource_name, value in obj.items():
            resource_matches = target_resource_name == resource_name

            if resource_matches:
                transformed_obj.update(value)
            else:
                transformed_obj[f"{resource_name}_id"] = value["id"]

            if resource_name == "ad_group_ad":
                transformed_obj.update(value["ad"])
                transformed_obj.pop('ad')

        if 'type_' in transformed_obj:
            LOGGER.info("Google sent us 'type_' when we asked for 'type', transforming this now")
            transformed_obj["type"] = transformed_obj.pop("type_")

        return transformed_obj


    def sync_core_streams(self, sdk_client, customer, stream):
        gas = sdk_client.get_service("GoogleAdsService", version=API_VERSION)
        resource_name = self.google_ads_resources_name[0]
        stream_name = stream["stream"]
        stream_mdata = stream["metadata"]

        query = create_core_stream_query(resource_name, stream_mdata)
        response = gas.search(query=query, customer_id=customer["customerId"])
        with Transformer() as transformer:
            # Pages are fetched automatically while iterating through the response
            for message in response:
                json_message = json.loads(MessageToJson(message, preserving_proto_field_name=True))
                transformed_obj = self.transform_keys(json_message)
                record = transformer.transform(transformed_obj, stream["schema"])
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


class ReportStream(BaseStream):
    def transform_keys(self, obj):
        transformed_obj = {}

        for resource_name, value in obj.items():
            if resource_name == "ad_group_ad":
                transformed_obj.update(value["ad"])
            else:
                transformed_obj.update(value)

        if 'type_' in transformed_obj:
            LOGGER.info("Google sent us 'type_' when we asked for 'type', transforming this now")
            transformed_obj["type"] = transformed_obj.pop("type_")

        return transformed_obj


    def sync_report_streams(self, sdk_client, customer, stream, config, STATE):
        gas = sdk_client.get_service("GoogleAdsService", version=API_VERSION)
        resource_name = self.google_ads_resources_name[0]
        stream_name = stream["stream"]
        stream_mdata = stream["metadata"]
        replication_key = 'date'
        STATE = singer.set_currently_syncing(STATE, stream_name)
        conversion_window = timedelta(days=int(config.get('conversion_window_days') or DEFAULT_CONVERSION_WINDOW))
        start_date = min(utils.strptime_to_utc(singer.get_bookmark(STATE, stream_name, replication_key, default=config['start_date'])),
                         utils.now() - conversion_window)
        end_date = utils.now()
        query_range = timedelta(days=NUM_DAYS_TO_QUERY)

        singer.write_state(STATE)

        while start_date < end_date:
            query_end_date = min(start_date + query_range, end_date)
            query = create_report_query(resource_name, stream_mdata, start_date, query_end_date)
            response = gas.search(query=query, customer_id=customer["customerId"])
            with Transformer() as transformer:
                # Pages are fetched automatically while iterating through the response
                for message in response:
                    json_message = json.loads(MessageToJson(message, preserving_proto_field_name=True))
                    transformed_obj = self.transform_keys(json_message)
                    record = transformer.transform(transformed_obj, stream["schema"])
                    _sdc_record_hash = generate_hash(record, stream_mdata)
                    record["_sdc_record_hash"] = _sdc_record_hash

                    singer.write_record(stream_name, record)

            singer.write_bookmark(STATE,
                                  stream_name,
                                  replication_key,
                                  utils.strftime(query_end_date))

            singer.write_state(STATE)

            start_date = query_end_date + timedelta(days=1)

        singer.write_state(STATE)

class AdGroupPerformanceReport(ReportStream):
    def add_extra_fields(self, resource_schema):
        # from the resource ad_group_ad_label
        field_name = "label.resource_name"
        # for field_name in []:
        self.field_exclusions[field_name] = {}
        self.schema[field_name] = {"type": ["null", "string"]}
        self.behavior[field_name] = "ATTRIBUTE"


class AdPerformanceReport(ReportStream):
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


class AudiencePerformanceReport(ReportStream):
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

class CampaignPerformanceReport(ReportStream):
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


class DisplayKeywordPerformanceReport(ReportStream):
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


class GeoPerformanceReport(ReportStream):
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


class KeywordsPerformanceReport(ReportStream):
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


class PlaceholderFeedItemReport(ReportStream):
    # TODO: The sync needs to select from feed_item_target if feed_item_target.device is selected
    # TODO: The sync needs to select from feed_item if feed_item.policy_infos is selected
    def add_extra_fields(self, resource_schema):
        for field_name in ["feed_item_target.device", "feed_item.policy_infos"]:
            self.field_exclusions[field_name] = set()
            self.schema[field_name] = {"type": ["null", "string"]}
            self.behavior[field_name] = "ATTRIBUTE"


def initialize_core_streams(resource_schema):
    return {
        "accounts": BaseStream(
            report_definitions.ACCOUNT_FIELDS,
            ["customer"],
            resource_schema,
            ["id"],
        ),
        "ad_groups": BaseStream(
            report_definitions.AD_GROUP_FIELDS,
            ["ad_group"],
            resource_schema,
            ["id"],
        ),
        "ads": BaseStream(
            report_definitions.AD_GROUP_AD_FIELDS,
            ["ad_group_ad"],
            resource_schema,
            ["id"],
        ),
        "campaigns": BaseStream(
            report_definitions.CAMPAIGN_FIELDS,
            ["campaign"],
            resource_schema,
            ["id"],
        ),
        "bidding_strategies": BaseStream(
            report_definitions.BIDDING_STRATEGY_FIELDS,
            ["bidding_strategy"],
            resource_schema,
            ["id"],
        ),
        "accessible_bidding_strategies": BaseStream(
            report_definitions.ACCESSIBLE_BIDDING_STRATEGY_FIELDS,
            ["accessible_bidding_strategy"],
            resource_schema,
            ["id"],
        ),
        "campaign_budgets": BaseStream(
            report_definitions.CAMPAIGN_BUDGET_FIELDS,
            ["campaign_budget"],
            resource_schema,
            ["id"],
        ),
    }


def initialize_reports(resource_schema):
    return {
        "account_performance_report": ReportStream(
            report_definitions.ACCOUNT_PERFORMANCE_REPORT_FIELDS,
            ["customer"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        # TODO: This needs to link with ad_group_ad_label
        # "adgroup_performance_report": AdGroupPerformanceReport(
        #     report_definitions.ADGROUP_PERFORMANCE_REPORT_FIELDS,
        #     ["ad_group"],
        #     resource_schema,
        #     ["_sdc_record_hash"],
        # ),
        "adgroup_performance_report": ReportStream(
            report_definitions.ADGROUP_PERFORMANCE_REPORT_FIELDS,
            ["ad_group"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        # "ad_performance_report": AdPerformanceReport(
        #     report_definitions.AD_PERFORMANCE_REPORT_FIELDS,
        #     ["ad_group_ad"],
        #     resource_schema,
        #     ["_sdc_record_hash"],
        # ),
        "ad_performance_report": ReportStream(
            report_definitions.AD_PERFORMANCE_REPORT_FIELDS,
            ["ad_group_ad"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "age_range_performance_report": ReportStream(
            report_definitions.AGE_RANGE_PERFORMANCE_REPORT_FIELDS,
            ["age_range_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        # "audience_performance_report": AudiencePerformanceReport(
        #     report_definitions.AUDIENCE_PERFORMANCE_REPORT_FIELDS,
        #     ["campaign_audience_view", "ad_group_audience_view"],
        #     resource_schema,
        #     ["_sdc_record_hash"],
        # ),
        "audience_performance_report": ReportStream(
            report_definitions.AD_GROUP_AUDIENCE_PERFORMANCE_REPORT_FIELDS,
            ["ad_group_audience_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "campaign_performance_report": ReportStream(
            report_definitions.CAMPAIGN_PERFORMANCE_REPORT_FIELDS,
            ["campaign"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "click_performance_report": ReportStream(
            report_definitions.CLICK_PERFORMANCE_REPORT_FIELDS,
            ["click_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        # "display_keyword_performance_report": DisplayKeywordPerformanceReport(
        #     report_definitions.DISPLAY_KEYWORD_PERFORMANCE_REPORT_FIELDS,
        #     ["display_keyword_view"],
        #     resource_schema,
        #     ["_sdc_record_hash"],
        # ),
        "display_keyword_performance_report": ReportStream(
            report_definitions.DISPLAY_KEYWORD_PERFORMANCE_REPORT_FIELDS,
            ["display_keyword_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        # "display_topics_performance_report": DisplayKeywordPerformanceReport(
        #     report_definitions.DISPLAY_TOPICS_PERFORMANCE_REPORT_FIELDS,
        #     ["topic_view"],
        #     resource_schema,
        #     ["_sdc_record_hash"],
        # ),
        "display_topics_performance_report": ReportStream(
            report_definitions.DISPLAY_TOPICS_PERFORMANCE_REPORT_FIELDS,
            ["topic_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "expanded_landing_page_report": ReportStream(
            report_definitions.EXPANDED_LANDING_PAGE_REPORT_FIELDS,
            ["expanded_landing_page_view"],
            resource_schema,
            ["_sdc_record_hash"]
        ),
        "gender_performance_report": ReportStream(
            report_definitions.GENDER_PERFORMANCE_REPORT_FIELDS,
            ["gender_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        # "geo_performance_report": GeoPerformanceReport(
        #     report_definitions.GEO_PERFORMANCE_REPORT_FIELDS,
        #     ["geographic_view", "user_location_view"],
        #     resource_schema,
        #     ["_sdc_record_hash"],
        # ),
        "geo_performance_report": ReportStream(
            report_definitions.GEO_PERFORMANCE_REPORT_FIELDS,
            ["geographic_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "keywordless_query_report": ReportStream(
            report_definitions.KEYWORDLESS_QUERY_REPORT_FIELDS,
            ["dynamic_search_ads_search_term_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        # "keywords_performance_report": KeywordsPerformanceReport(
        #     report_definitions.KEYWORDS_PERFORMANCE_REPORT_FIELDS,
        #     ["keyword_view"],
        #     resource_schema,
        #     ["_sdc_record_hash"],
        # ),
        "keywords_performance_report": ReportStream(
            report_definitions.KEYWORDS_PERFORMANCE_REPORT_FIELDS,
            ["keyword_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "landing_page_report": ReportStream(
            report_definitions.LANDING_PAGE_REPORT_FIELDS,
            ["landing_page_view"],
            resource_schema,
            ["_sdc_record_hash"]
        ),
        # "placeholder_feed_item_report": PlaceholderFeedItemReport(
        #     report_definitions.PLACEHOLDER_FEED_ITEM_REPORT_FIELDS,
        #     ["feed_item", "feed_item_target"],
        #     resource_schema,
        #     ["_sdc_record_hash"],
        # ),
        "placeholder_feed_item_report": ReportStream(
            report_definitions.PLACEHOLDER_FEED_ITEM_REPORT_FIELDS,
            ["feed_item"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "placeholder_report": ReportStream(
            report_definitions.PLACEHOLDER_REPORT_FIELDS,
            ["feed_placeholder_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "placement_performance_report": ReportStream(
            report_definitions.PLACEMENT_PERFORMANCE_REPORT_FIELDS,
            ["managed_placement_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "search_query_performance_report": ReportStream(
            report_definitions.SEARCH_QUERY_PERFORMANCE_REPORT_FIELDS,
            ["search_term_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "shopping_performance_report": ReportStream(
            report_definitions.SHOPPING_PERFORMANCE_REPORT_FIELDS,
            ["shopping_performance_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "video_performance_report": ReportStream(
            report_definitions.VIDEO_PERFORMANCE_REPORT_FIELDS,
            ["video"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
    }
