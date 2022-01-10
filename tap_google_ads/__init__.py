#!/usr/bin/env python3
import json
import os
import re
import sys

import singer
from singer import utils
from singer import bookmarks
from singer import metadata
from singer import transform, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING, Transformer

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf.json_format import MessageToJson


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "oauth_client_id",
    "oauth_client_secret",
    "refresh_token",
    "customer_ids",
    "developer_token",
]

CORE_ENDPOINT_MAPPINGS = {
    "campaign": {"primary_keys": ["id"], "stream_name": "campaigns"},
    "ad_group": {"primary_keys": ["id"], "stream_name": "ad_groups"},
    "ad_group_ad": {"primary_keys": ["id"], "stream_name": "ads"},
    "customer": {"primary_keys": ["id"], "stream_name": "accounts"},
}

REPORTS = [
    "age_range_view",
    "campaign_audience_view",
    "call_view",
    "click_view",
    "display_keyword_view",
    "topic_view",
    "gender_view",
    "geographic_view",
    "user_location_view",
    "dynamic_search_ads_search_term_view",
    "keyword_view",
    "landing_page_view",
    "expanded_landing_page_view",
    "feed_item",
    "feed_item_target",
    "feed_placeholder_view",
    "managed_placement_view",
    "search_term_view",
    "shopping_performance_view",
    "video",
]

CATEGORY_MAP = {
    0: "UNSPECIFIED",
    1: "UNKNOWN",
    2: "RESOURCE",
    3: "ATTRIBUTE",
    5: "SEGMENT",
    6: "METRIC",
}


def get_attributes(api_objects, resource):
    resource_attributes = []

    if CATEGORY_MAP[resource.category] != "RESOURCE":
        # Attributes, segments, and metrics do not have attributes
        return resource_attributes

    attributed_resources = set(resource.attribute_resources)
    for field in api_objects:
        root_object_name = field.name.split(".")[0]
        does_field_exist_on_resource = (
            root_object_name == resource.name
            or root_object_name in attributed_resources
        )
        is_field_an_attribute = CATEGORY_MAP[field.category] == "ATTRIBUTE"
        if is_field_an_attribute and does_field_exist_on_resource:
            resource_attributes.append(field.name)
    return resource_attributes


def create_resource_schema(config):
    client = GoogleAdsClient.load_from_dict(get_client_config(config))
    gaf_service = client.get_service("GoogleAdsFieldService")

    query = "SELECT name, category, data_type, selectable, filterable, sortable, selectable_with, metrics, segments, is_repeated, type_url, enum_values, attribute_resources"

    api_objects = gaf_service.search_google_ads_fields(query=query)

    # These are the data types returned from google. They are mapped to json schema. UNSPECIFIED and UNKNOWN have never been returned.
    # 0: "UNSPECIFIED", 1: "UNKNOWN", 2: "BOOLEAN", 3: "DATE", 4: "DOUBLE", 5: "ENUM", 6: "FLOAT", 7: "INT32", 8: "INT64", 9: "MESSAGE", 10: "RESOURCE_NAME", 11: "STRING", 12: "UINT64"
    data_type_map = {
        0: {"type": ["null", "string"]},
        1: {"type": ["null", "string"]},
        2: {"type": ["null", "boolean"]},
        3: {"type": ["null", "string"]},
        4: {"type": ["null", "string"], "format": "singer.decimal"},
        5: {"type": ["null", "string"]},
        6: {"type": ["null", "string"], "format": "singer.decimal"},
        7: {"type": ["null", "integer"]},
        8: {"type": ["null", "integer"]},
        9: {"type": ["null", "object"], "properties": {}},
        10: {"type": ["null", "object"], "properties": {}},
        11: {"type": ["null", "string"]},
        12: {"type": ["null", "integer"]},
    }

    resource_schema = {}

    for resource in api_objects:

        attributes = get_attributes(api_objects, resource)

        resource_metadata = {
            "name": resource.name,
            "category": CATEGORY_MAP[resource.category],
            "json_schema": data_type_map[resource.data_type],
            "selectable": resource.selectable,
            "filterable": resource.filterable,
            "sortable": resource.sortable,
            "selectable_with": set(resource.selectable_with),
            "metrics": list(resource.metrics),
            "segments": list(resource.segments),
            "attributes": attributes,
        }
        resource_schema[resource.name] = resource_metadata

    for report in REPORTS:
        report_object = resource_schema[report]
        fields = {}
        attributes = report_object["attributes"]
        metrics = report_object["metrics"]
        segments = report_object["segments"]
        for field in attributes + metrics + segments:
            field_schema = resource_schema[field]
            fields[field_schema["name"]] = {
                "field_details": field_schema,
                "incompatible_fields": [],
            }

        metrics_and_segments = set(metrics + segments)
        for field_name, field in fields.items():
            for compared_field in metrics_and_segments:
                if (
                    field_name != compared_field
                    and field_name
                    not in resource_schema[compared_field]["selectable_with"]
                ):
                    field["incompatible_fields"].append(compared_field)
        report_object["fields"] = fields
    return resource_schema


def canonicalize_name(name):
    """Remove all dot and underscores and camel case the name."""
    tokens = re.split("\\.|_", name)

    first_word = [tokens[0]]
    other_words = [word.capitalize() for word in tokens[1:]]

    return "".join(first_word + other_words)


def do_discover_reports(resource_schema):
    catalog = []
    for report in REPORTS:
        report_object = resource_schema[report]
        fields = report_object["fields"]
        report_schema = {}
        report_metadata = {}

        for field, props in fields.items():
            the_schema = props["field_details"]["json_schema"]
            report_schema[field] = the_schema
            report_metadata[("properties", field)] = {
                "inclusion": "available",
                "fieldExclusions": props["incompatible_fields"],
                "behavior": props["field_details"]["category"],
            }

        catalog_entry = {
            "tap_stream_id": report,
            "stream": report,
            "schema": {
                "type": ["null", "object"],
                "is_report": True,
                "properties": report_schema,
            },
            "metadata": singer.metadata.to_list(report_metadata),
        }
        catalog.append(catalog_entry)
    return catalog


def create_field_metadata(primary_key, schema):
    mdata = {}
    mdata = metadata.write(mdata, (), "inclusion", "available")
    mdata = metadata.write(mdata, (), "table-key-properties", primary_key)

    for field in schema["properties"]:
        breadcrumb = ("properties", str(field))
        mdata = metadata.write(mdata, breadcrumb, "inclusion", "available")

    mdata = metadata.write(
        mdata, ("properties", primary_key[0]), "inclusion", "automatic"
    )
    mdata = metadata.to_list(mdata)

    return mdata


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    return utils.load_json(get_abs_path(f"schemas/{entity}.json"))


def load_metadata(entity):
    return utils.load_json(get_abs_path(f"metadata/{entity}.json"))


def do_discover_core_streams():
    streams = []
    LOGGER.info("Starting core discovery")
    for resource_name, properties in CORE_ENDPOINT_MAPPINGS.items():
        LOGGER.info("Loading schema for %s", resource_name)
        stream_name = properties["stream_name"]
        schema = load_schema(stream_name)
        md = create_field_metadata(properties["primary_keys"], schema)
        streams.append(
            {
                "stream": stream_name,
                "tap_stream_id": stream_name,
                "schema": schema,
                "metadata": md,
            }
        )
    return streams


def do_discover(config):
    resource_schema = create_resource_schema(config)
    core_streams = do_discover_core_streams()
    report_streams = do_discover_reports(resource_schema)
    streams = []
    streams.extend(core_streams)
    streams.extend(report_streams)
    json.dump({"streams": streams}, sys.stdout, indent=2)


def create_sdk_client(config):
    CONFIG = {
        "use_proto_plus": False,
        "developer_token": config["developer_token"],
        "client_id": config["oauth_client_id"],
        "client_secret": config["oauth_client_secret"],
        "access_token": config["access_token"],
        "refresh_token": config["refresh_token"],
    }
    sdk_client = GoogleAdsClient.load_from_dict(CONFIG)
    return sdk_client


def get_client_config(config, login_customer_id=None):

    if login_customer_id:
        return {
            "use_proto_plus": False,
            "developer_token": config["developer_token"],
            "client_id": config["oauth_client_id"],
            "client_secret": config["oauth_client_secret"],
            "refresh_token": config["refresh_token"],
            "login_customer_id": login_customer_id,
        }
    else:
        return {
            "use_proto_plus": False,
            "developer_token": config["developer_token"],
            "client_id": config["oauth_client_id"],
            "client_secret": config["oauth_client_secret"],
            "refresh_token": config["refresh_token"],
        }


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
        LOGGER.info("Discovery complete")


if __name__ == "__main__":
    main()
