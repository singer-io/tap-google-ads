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

REPORTS = [
    "accessible_bidding_strategy",
    "ad_group",
    "ad_group_ad",
    "ad_group_audience_view",
    "age_range_view",
    "bidding_strategy",
    "call_view",
    "campaign",
    "campaign_audience_view",
    "campaign_budget",
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

STATE = {}

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
            "json_schema": dict(data_type_map[resource.data_type]),
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
            if field["field_details"]["category"] == "ATTRIBUTE":
                continue
            for compared_field in metrics_and_segments:

                if not (
                    field_name.startswith("segments.")
                    or field_name.startswith("metrics.")
                ):
                    field_root_resource = field_name.split(".")[0]
                else:
                    field_root_resource = None

                if not (compared_field.startswith("segments.") or compared_field.startswith("metrics.")):
                    compared_field_root_resource = compared_field.split(".")[0]
                else:
                    compared_field_root_resource = None

                if (field_name != compared_field and not compared_field.startswith(f"{field_root_resource}.")) and (
                    fields[compared_field]["field_details"]["category"] == "METRIC"
                    or fields[compared_field]["field_details"]["category"] == "SEGMENT"
                ):
                    field_to_check = field_root_resource or field_name
                    compared_field_to_check = compared_field_root_resource or compared_field

                    if field_name.startswith('metrics.') and compared_field.startswith('metrics.'):
                        continue
                    elif (
                        field_to_check
                        not in resource_schema[compared_field_to_check]["selectable_with"]
                    ):
                        field["incompatible_fields"].append(compared_field)

        report_object["fields"] = fields
    return resource_schema


def create_nested_resource_schema(resource_schema, fields):
    new_schema = {
        "type": ["null", "object"],
        "properties": {}
    }

    for field in fields:
        walker = new_schema["properties"]
        paths = field.split(".")
        last_path = paths[-1]
        for path in paths[:-1]:
            if path not in walker:
                walker[path] = {
                    "type": ["null", "object"],
                    "properties": {}
                }
            walker = walker[path]["properties"]
        if last_path not in walker:
            json_schema = resource_schema[field]["json_schema"]
            walker[last_path] = json_schema
    return new_schema

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
        google_ads_name = stream.google_ads_resources_name[0]
        resource_object = resource_schema[google_ads_name]
        fields = resource_object["fields"]
        full_schema = create_nested_resource_schema(resource_schema, fields)
        report_schema = full_schema["properties"][google_ads_name]

        for attributed_resource, schema in full_schema['properties'].items():
            # ads stream is special since all of the ad fields are nested under ad_group_ad.ad
            # we need to bump the fields up a level so they are selectable
            if attributed_resource == 'ad_group_ad':
                for ad_field_name, ad_field_schema in full_schema['properties']['ad_group_ad']['properties']['ad']['properties'].items():
                    report_schema['properties'][ad_field_name] = ad_field_schema
                report_schema['properties'].pop('ad')
            if attributed_resource not in {"metrics", "segments", google_ads_name}:
                report_schema["properties"][attributed_resource + "_id"] = schema["properties"]["id"]

        report_metadata = {
            (): {
                "inclusion": "available",
                "table-key-properties": stream.primary_keys,
                "table-foreign-key-properties": [],
            }
        }

        for field, props in fields.items():
            resource_matches = field.startswith(resource_object["name"] + ".")
            is_id_field = field.endswith(".id")
            if props["field_details"]["category"] == "ATTRIBUTE" and (
                resource_matches or is_id_field
            ):
                # Transform the field name to match the schema
                # Special case for ads since they are nested under ad_group_ad
                # we have to bump them up a level
                if field.startswith("ad_group_ad.ad."):
                    field = field.split(".")[2]
                else:
                    if resource_matches:
                        field = field.split(".")[1]
                    elif is_id_field:
                        field = field.replace(".", "_")
                        report_metadata[()]["table-foreign-key-properties"].append(field)


                if ("properties", field) not in report_metadata:
                    # Base metadata for every field
                    report_metadata[("properties", field)] = {
                        "fieldExclusions": props["incompatible_fields"],
                        "behavior": props["field_details"]["category"],
                    }

                    # Add inclusion metadata
                    if field in stream.primary_keys:
                        inclusion = "automatic"
                    elif props["field_details"]["selectable"]:
                        inclusion = "available"
                    else:
                        # inclusion = "unsupported"
                        continue
                    report_metadata[("properties", field)]["inclusion"] = inclusion

                # Save the full field name for sync code to use
                full_name = props["field_details"]["name"]
                if "fields_to_sync" not in report_metadata[("properties", field)]:
                    report_metadata[("properties", field)]["fields_to_sync"] = []

                if props['field_details']['selectable']:
                    report_metadata[("properties", field)]["fields_to_sync"].append(full_name)

        catalog_entry = {
            "tap_stream_id": stream.google_ads_resources_name[0],
            "stream": stream_name,
            "schema": report_schema,
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
    report_streams = initialize_reports(resource_schema)

    for customer in customers:
        sdk_client = create_sdk_client(config, customer["loginCustomerId"])
        for catalog_entry in selected_streams:
            stream_name = catalog_entry["stream"]
            if stream_name in core_streams:
                stream_obj = core_streams[stream_name]

                mdata_map = singer.metadata.to_map(catalog_entry["metadata"])

                primary_key = (
                    mdata_map[()].get("table-key-properties", [])
                )

                singer.messages.write_schema(
                    stream_name, catalog_entry["schema"], primary_key
                )
                stream_obj.sync(sdk_client, customer, catalog_entry, state)
            else:
                # syncing report
                stream_obj = report_streams[stream_name]
                mdata_map = singer.metadata.to_map(catalog_entry["metadata"])
                singer.messages.write_schema(stream_name, catalog_entry["schema"], [])
                stream_obj.sync(sdk_client, customer, catalog_entry, config, STATE)


def do_discover(resource_schema):
    core_streams = do_discover_core_streams(resource_schema)
    report_streams = do_discover_reports(resource_schema)
    streams = []
    streams.extend(core_streams)
    streams.extend(report_streams)
    json.dump({"streams": streams}, sys.stdout, indent=2)


def strip_prefix(field_name):
    return field_name.replace('segments.', '').replace('metrics.', '')

def do_discover_reports(resource_schema):
    ADWORDS_TO_GOOGLE_ADS = initialize_reports(resource_schema)

    streams = []
    for adwords_report_name, report in ADWORDS_TO_GOOGLE_ADS.items():
        report_metadata = {tuple(): {"inclusion": "available"}}

        full_schema = create_nested_resource_schema(resource_schema, report.fields)
        report_schema = {
            "type": ["null", "object"],
            "is_report": True,
            "properties": {},
        }

        # TODO repeat this logic for report sync
        for resource_name, schema in full_schema['properties'].items():
            for key, val in schema['properties'].items():

                is_metric_or_segment = key.startswith("metrics.") or key.startswith("segments.")
                if resource_name not in {"metrics", "segments"} and resource_name not in report.google_ads_resources_name:
                    report_schema['properties'][f"{resource_name}_{key}"] = val
                # Move ad_group_ad.ad.x fields up a level in the schema (ad_group_ad.ad.x -> ad_group_ad.x)
                elif resource_name == 'ad_group_ad' and key == 'ad':
                    for ad_field_name, ad_field_schema in val['properties'].items():
                        report_schema['properties'][ad_field_name] = ad_field_schema
                else:
                    report_schema['properties'][key] = val

        for report_field in report.fields:
            # Transform the field name to match the schema
            is_metric_or_segment = report_field.startswith("metrics.") or report_field.startswith("segments.")
            if not is_metric_or_segment and report_field.split(".")[0] not in report.google_ads_resources_name:
                transformed_field_name = "_".join(report_field.split(".")[:2])
            # Transform ad_group_ad.ad.x fields to just x to reflect ad_group_ads schema
            elif report_field.startswith('ad_group_ad.ad.'):
                transformed_field_name = report_field.split(".")[2]
            else:
                transformed_field_name = report_field.split(".")[1]

            # Base metadata for every field
            if ("properties", transformed_field_name) not in report_metadata:
                report_metadata[("properties", transformed_field_name)] = {
                    "fieldExclusions": [],
                    "behavior": report.behavior[report_field],
                }

                # Transform field exclusion names so they match the schema
                for field_name in report.field_exclusions[report_field]:
                    is_metric_or_segment = field_name.startswith("metrics.") or field_name.startswith("segments.")
                    if not is_metric_or_segment and field_name.split(".")[0] not in report.google_ads_resources_name:
                        new_field_name = field_name.replace(".", "_")
                    else:
                        new_field_name = field_name.split(".")[1]

                    report_metadata[("properties", transformed_field_name)]["fieldExclusions"].append(new_field_name)

            # Add inclusion metadata
            if report.behavior[report_field]:
                inclusion = "available"
                if report_field == "segments.date":
                    inclusion = "automatic"
            else:
                inclusion = "unsupported"
            report_metadata[("properties", transformed_field_name)]["inclusion"] = inclusion

            # Save the full field name for sync code to use
            if "fields_to_sync" not in report_metadata[("properties", transformed_field_name)]:
                report_metadata[("properties", transformed_field_name)]["fields_to_sync"] = []

            report_metadata[("properties", transformed_field_name)]["fields_to_sync"].append(report_field)

        catalog_entry = {
            "tap_stream_id": adwords_report_name,
            "stream": adwords_report_name,
            "schema": report_schema,
            "metadata": singer.metadata.to_list(report_metadata),
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
    if args.state:
        STATE = args.state
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
