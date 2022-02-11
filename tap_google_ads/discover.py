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

from tap_google_ads.client import create_sdk_client
from tap_google_ads.streams import initialize_core_streams
from tap_google_ads.streams import initialize_reports

API_VERSION = "v9"

LOGGER = singer.get_logger()

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
    "campaign_criterion",
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


def get_api_objects(config):
    client = create_sdk_client(config)
    gaf_service = client.get_service("GoogleAdsFieldService")

    query = "SELECT name, category, data_type, selectable, filterable, sortable, selectable_with, metrics, segments, is_repeated, type_url, enum_values, attribute_resources"

    api_objects = gaf_service.search_google_ads_fields(query=query)
    return api_objects


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


def build_resource_metadata(api_objects, resource):
    attributes = get_attributes(api_objects, resource)

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

    return resource_metadata


def get_root_resource_name(field_name):
    if not (
        field_name.startswith("segments.")
        or field_name.startswith("metrics.")
    ):
        field_root_resource = field_name.split(".")[0]
    else:
        field_root_resource = field_name

    return field_root_resource


def create_resource_schema(config):
    """
    The resource schema is necessary to create a 'source of truth' with regards to the fields
    Google Ads can return to us. It allows for the discovery of field exclusions and other fun
    things like data types.


    It includes every field Google Ads can return and the possible fields that each resource
    can return.

    This schema is based off of the Google Ads blog posts for the creation of their query builder:
    https://ads-developers.googleblog.com/2021/04/the-query-builder-blog-series-part-3.html
    """

    resource_schema = {}

    api_objects = get_api_objects(config)
    
    for resource in api_objects:
        resource_schema[resource.name] = build_resource_metadata(api_objects, resource)

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

        # Start discovery of field exclusions
        metrics_and_segments = set(metrics + segments)
        all_fields = fields.items()
        # do_field_exclusions(all_fields, metrics_and_segments)
        
        for field_name, field in fields.items():
            if field["field_details"]["category"] == "ATTRIBUTE":
                continue
            for compared_field in metrics_and_segments:
                field_root_resource = get_root_resource_name(field_name)
                compared_field_root_resource = get_root_resource_name(compared_field)

                #Fields can be any of the categories in CATEGORY_MAP, but only METRIC & SEGMENT have exclusions, so only check those
                if (field_name != compared_field and not compared_field.startswith(f"{field_root_resource}.")) and (
                    fields[compared_field]["field_details"]["category"] == "METRIC"
                    or fields[compared_field]["field_details"]["category"] == "SEGMENT"
                ):

                    field_to_check = field_root_resource or field_name
                    compared_field_to_check = compared_field_root_resource or compared_field

                    #Metrics will not be incompatible with other metrics, so don't check those
                    if field_name.startswith('metrics.') and compared_field.startswith('metrics.'):
                        continue

                    # If a resource is selectable with another resource they should be in
                    # each other's 'selectable_with' list, but Google is missing some of
                    # these so we have to check both ways
                    if (
                        field_to_check
                        not in resource_schema[compared_field_to_check]["selectable_with"]
                        and compared_field_to_check
                        not in resource_schema[field_to_check]["selectable_with"]
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


def do_discover_core_streams(resource_schema):
    stream_name_to_resource = initialize_core_streams(resource_schema)

    catalog = []
    for stream_name, stream in stream_name_to_resource.items():
        google_ads_name = stream.google_ads_resource_names[0]
        resource_object = resource_schema[google_ads_name]
        fields = resource_object["fields"]
        full_schema = create_nested_resource_schema(resource_schema, fields)
        report_schema = full_schema["properties"][google_ads_name]

        report_metadata = {
            (): {
                "inclusion": "available",
                "table-key-properties": stream.primary_keys,
            }
        }

        
        # TODO refactor
        for resource_name, schema in full_schema['properties'].items():
            # ads stream is special since all of the ad fields are nested under ad_group_ad.ad
            # we need to bump the fields up a level so they are selectable
            if resource_name == 'ad_group_ad':
                for ad_field_name, ad_field_schema in full_schema['properties']['ad_group_ad']['properties']['ad']['properties'].items():
                    schema['properties'][ad_field_name] = ad_field_schema
                schema['properties'].pop('ad')
            if resource_name not in {"metrics", "segments", google_ads_name}:
                schema["properties"][resource_name + "_id"] = schema["properties"]["id"]


        # TODO refactor
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

                if ("properties", field) not in report_metadata:
                    # Base metadata for every field
                    report_metadata[("properties", field)] = {
                        "fieldExclusions": props["incompatible_fields"],
                        "behavior": props["field_details"]["category"],
                    }

                    # Add inclusion metadata
                    # Foreign keys are automatically included and they are all id fields
                    if field in stream.primary_keys or is_id_field:
                        inclusion = "automatic"
                    elif props["field_details"]["selectable"]:
                        inclusion = "available"
                    else:
                        # inclusion = "unsupported"
                        continue
                    report_metadata[("properties", field)]["inclusion"] = inclusion

                # Save the full field name for sync code to use
                full_name = props["field_details"]["name"]
                if "tap-google-ads.api-field-names" not in report_metadata[("properties", field)]:
                    report_metadata[("properties", field)]["tap-google-ads.api-field-names"] = []

                if props['field_details']['selectable']:
                    report_metadata[("properties", field)]["tap-google-ads.api-field-names"].append(full_name)

        catalog_entry = {
            "tap_stream_id": stream_name,
            "stream": stream_name,
            "schema": report_schema,
            "metadata": singer.metadata.to_list(report_metadata),
        }
        catalog.append(catalog_entry)
    return catalog


def do_discover_reports(resource_schema):
    stream_name_to_resource = initialize_reports(resource_schema)

    streams = []
    for stream_name, report in stream_name_to_resource.items():
        report_metadata = {
            (): {"inclusion": "available",
                 "table-key-properties": ["_sdc_record_hash"]},
            ("properties", "_sdc_record_hash"):
                {"inclusion": "automatic"}
        }

        full_schema = create_nested_resource_schema(resource_schema, report.fields)
        report_schema = {
            "type": ["null", "object"],
            "is_report": True,
            "properties": {"_sdc_record_hash": {"type": "string"}},
        }

        # TODO refactor
        for resource_name, schema in full_schema['properties'].items():
            for field_name, data_type in schema['properties'].items():
                # Ensure that attributed resource fields have the resource name as a prefix, eg campaign_id under the ad_groups stream
                if resource_name not in {"metrics", "segments"} and resource_name not in report.google_ads_resource_names:
                    report_schema['properties'][f"{resource_name}_{field_name}"] = data_type
                # Move ad_group_ad.ad.x fields up a level in the schema (ad_group_ad.ad.x -> ad_group_ad.x)
                elif resource_name == 'ad_group_ad' and field_name == 'ad':
                    for ad_field_name, ad_field_schema in data_type['properties'].items():
                        report_schema['properties'][ad_field_name] = ad_field_schema
                else:
                    report_schema['properties'][field_name] = data_type

        # TODO refactor
        for report_field in report.fields:
            # Transform the field name to match the schema
            is_metric_or_segment = report_field.startswith("metrics.") or report_field.startswith("segments.")
            if not is_metric_or_segment and report_field.split(".")[0] not in report.google_ads_resource_names:
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
                    if not is_metric_or_segment and field_name.split(".")[0] not in report.google_ads_resource_names:
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
            if "tap-google-ads.api-field-names" not in report_metadata[("properties", transformed_field_name)]:
                report_metadata[("properties", transformed_field_name)]["tap-google-ads.api-field-names"] = []

            report_metadata[("properties", transformed_field_name)]["tap-google-ads.api-field-names"].append(report_field)

        catalog_entry = {
            "tap_stream_id": stream_name,
            "stream": stream_name,
            "schema": report_schema,
            "metadata": singer.metadata.to_list(report_metadata),
        }
        streams.append(catalog_entry)

    return streams


def do_discover(resource_schema):
    core_streams = do_discover_core_streams(resource_schema)
    report_streams = do_discover_reports(resource_schema)
    streams = []
    streams.extend(core_streams)
    streams.extend(report_streams)
    json.dump({"streams": streams}, sys.stdout, indent=2)
