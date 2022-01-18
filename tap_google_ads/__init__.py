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

from tap_google_ads.reports import initialize_core_streams
from tap_google_ads.reports import initialize_reports

API_VERSION = "v9"

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

CORE_STREAMS = ["Campaigns", "Ad_Groups", "Ads", "Accounts"]

REPORTS = [
    "ad_group",
    "ad_group_ad",
    "ad_group_audience_view",
    "age_range_view",
    "call_view",
    "campaign",
    "campaign_audience_view",
    "click_view",
    "customer",
    "display_keyword_view",
    "dynamic_search_ads_search_term_view",
    "expanded_landing_page_view",
    "feed_item",
    "feed_item_target",
    "feed_placeholder_view",
    "gender_view",
    "geographic_view",
    "keyword_view",
    "landing_page_view",
    "managed_placement_view",
    "search_term_view",
    "shopping_performance_view",
    "topic_view",
    "user_location_view",
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


def get_segments(resource_schema, resource):
    resource_segments = []

    if resource["category"] != "RESOURCE":
        # Attributes, segments, and metrics do not have attributes
        return resource_segments

    segments = resource["segments"]
    for segment in segments:
        if segment.startswith("segments."):
            resource_segments.append(segment)
        else:
            segment_schema = resource_schema[segment]
            segment_attributes = [
                attribute
                for attribute in segment_schema["attributes"]
                if attribute.startswith(f"{segment}.")
            ]
            resource_segments.extend(segment_attributes)
    return resource_segments


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
        9: {"type": ["null", "object", "string"], "properties": {}},
        10: {"type": ["null", "object", "string"], "properties": {}},
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

    for resource in resource_schema.values():
        updated_segments = get_segments(resource_schema, resource)
        resource["segments"] = updated_segments

    for report in REPORTS:
        report_object = resource_schema[report]
        fields = {}
        attributes = report_object["attributes"]
        metrics = report_object["metrics"]
        segments = report_object["segments"]
        for field in attributes + metrics + segments:
            field_schema = dict(resource_schema[field])

            if field_schema["name"] in segments:
                field_schema["category"] = "SEGMENT"

            fields[field_schema["name"]] = {
                "field_details": field_schema,
                "incompatible_fields": [],
            }

        metrics_and_segments = set(metrics + segments)
        for field_name, field in fields.items():
            for compared_field in metrics_and_segments:

                if not (
                    field_name.startswith("segments.")
                    or field_name.startswith("metrics.")
                ):
                    field_root_resource = field_name.split(".")[0]
                else:
                    field_root_resource = None

                if (field_name != compared_field) and (
                    compared_field.startswith("metrics.")
                    or compared_field.startswith("segments.")
                ):
                    field_to_check = field_root_resource or field_name
                    if (
                        field_to_check
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


def do_discover_core_streams(resource_schema):
    adwords_to_google_ads = initialize_core_streams(resource_schema)

    catalog = []
    for stream_name, stream in adwords_to_google_ads.items():
        resource_object = resource_schema[stream.google_ads_resources_name[0]]
        fields = resource_object["fields"]
        report_schema = {}
        report_metadata = {
            tuple(): {
                "inclusion": "available",
                "table-key-properties": stream.primary_keys,
            }
        }

        for field, props in fields.items():
            if props["field_details"]["category"] == "ATTRIBUTE":
                the_schema = props["field_details"]["json_schema"]
                report_schema[field] = the_schema
                report_metadata[("properties", field)] = {
                    "fieldExclusions": props["incompatible_fields"],
                    "behavior": props["field_details"]["category"],
                }
                if field in stream.primary_keys:
                    inclusion = "automatic"
                elif props["field_details"]["selectable"]:
                    inclusion = "available"
                else:
                    inclusion = "unsupported"
                report_metadata[("properties", field)]["inclusion"] = inclusion

        catalog_entry = {
            "tap_stream_id": stream.google_ads_resources_name[0],
            "stream": stream_name,
            "schema": {
                "type": ["null", "object"],
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


def create_sdk_client(config, login_customer_id=None):
    CONFIG = {
        "use_proto_plus": False,
        "developer_token": config["developer_token"],
        "client_id": config["oauth_client_id"],
        "client_secret": config["oauth_client_secret"],
        "access_token": config["access_token"],
        "refresh_token": config["refresh_token"],
    }

    if login_customer_id:
        CONFIG["login_customer_id"] = login_customer_id

    sdk_client = GoogleAdsClient.load_from_dict(CONFIG)
    return sdk_client


def do_sync(config, catalog, resource_schema):
    customers = json.loads(config["login_customer_ids"])

    selected_streams = [
        stream
        for stream in catalog["streams"]
        if singer.metadata.to_map(stream["metadata"])[()].get("selected")
    ]

    core_streams = initialize_core_streams(resource_schema)

    for customer in customers:
        sdk_client = create_sdk_client(config, customer["loginCustomerId"])
        for catalog_entry in selected_streams:
            stream_name = catalog_entry["stream"]
            if stream_name in core_streams:
                stream_obj = core_streams[stream_name]

                mdata_map = singer.metadata.to_map(catalog_entry["metadata"])

                primary_key = (
                    mdata_map[()].get("metadata", {}).get("table-key-properties", [])
                )
                singer.messages.write_schema(stream_name, catalog_entry["schema"], primary_key)
                stream_obj.sync(
                    sdk_client, customer, catalog_entry
                )


def do_discover(resource_schema):
    core_streams = do_discover_core_streams(resource_schema)
    # report_streams = do_discover_reports(resource_schema)
    streams = []
    streams.extend(core_streams)
    # streams.extend(report_streams)
    json.dump({"streams": streams}, sys.stdout, indent=2)


def do_discover_reports(resource_schema):
    ADWORDS_TO_GOOGLE_ADS = initialize_reports(resource_schema)

    streams = []
    for adwords_report_name, report in ADWORDS_TO_GOOGLE_ADS.items():
        report_mdata = {tuple(): {"inclusion": "available"}}
        try:
            for report_field in report.fields:
                # field  = resource_schema[report_field]
                report_mdata[("properties", report_field)] = {
                    # "fieldExclusions": report.field_exclusions.get(report_field, []),
                    # "behavior": report.behavior.get(report_field, "ATTRIBUTE"),
                    "fieldExclusions": report.field_exclusions[report_field],
                    "behavior": report.behavior[report_field],
                }

                if report.behavior[report_field]:
                    inclusion = "available"
                else:
                    inclusion = "unsupported"
                report_mdata[("properties", report_field)]["inclusion"] = inclusion
        except Exception as err:
            print(f"Error in {adwords_report_name}")
            raise err

        catalog_entry = {
            "tap_stream_id": adwords_report_name,
            "stream": adwords_report_name,
            "schema": {
                "type": ["null", "object"],
                "is_report": True,
                "properties": report.schema,
            },
            "metadata": singer.metadata.to_list(report_mdata),
        }
        streams.append(catalog_entry)

    return streams


def get_client_config(config, login_customer_id=None):
    client_config = {
        "use_proto_plus": False,
        "developer_token": config["developer_token"],
        "client_id": config["oauth_client_id"],
        "client_secret": config["oauth_client_secret"],
        "refresh_token": config["refresh_token"],
        # "access_token": config["access_token"],
    }

    if login_customer_id:
        client_config["login_customer_id"] = login_customer_id

    return client_config


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    resource_schema = create_resource_schema(args.config)
    if args.discover:
        do_discover(resource_schema)
        LOGGER.info("Discovery complete")
    elif args.catalog:
        do_sync(args.config, args.catalog.to_dict(), resource_schema)
        LOGGER.info("Sync Completed")
    else:
        LOGGER.info("No properties were selected")


if __name__ == "__main__":
    main()
