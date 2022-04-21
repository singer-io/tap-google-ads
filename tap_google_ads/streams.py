from collections import defaultdict
import json
import hashlib
from datetime import timedelta
import singer
from singer import Transformer
from singer import utils
from google.protobuf.json_format import MessageToJson
from google.ads.googleads.errors import GoogleAdsException
import backoff
from . import report_definitions

LOGGER = singer.get_logger()

API_VERSION = "v10"

API_PARAMETERS = {
    "omit_unselected_resource_names": "true"
}

REPORTS_WITH_90_DAY_MAX = frozenset(
    [
        "click_performance_report",
    ]
)

DEFAULT_CONVERSION_WINDOW = 30


def get_conversion_window(config):
    """Fetch the conversion window from the config and error on invalid values"""
    conversion_window = config.get("conversion_window") or DEFAULT_CONVERSION_WINDOW

    try:
        conversion_window = int(conversion_window)
    except (ValueError, TypeError) as err:
        raise RuntimeError("Conversion Window must be an int or string") from err

    if conversion_window in set(range(1,31)) or conversion_window in {60, 90}:
        return conversion_window

    raise RuntimeError("Conversion Window must be between 1 - 30 inclusive, 60, or 90")

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


def get_selected_fields(stream_mdata):
    selected_fields = set()
    for mdata in stream_mdata:
        if mdata["breadcrumb"]:
            inclusion = mdata["metadata"].get("inclusion")
            selected = mdata["metadata"].get("selected")
            if utils.should_sync_field(inclusion, selected) and mdata["breadcrumb"][1] != "_sdc_record_hash":
                selected_fields.update(mdata["metadata"]["tap-google-ads.api-field-names"])

    return selected_fields


def build_parameters():
    param_str = ",".join(f"{k}={v}" for k, v in API_PARAMETERS.items())
    return f"PARAMETERS {param_str}"


def create_core_stream_query(resource_name, selected_fields):
    core_query = f"SELECT {','.join(selected_fields)} FROM {resource_name} {build_parameters()}"
    return core_query


def create_report_query(resource_name, selected_fields, query_date):

    format_str = "%Y-%m-%d"
    query_date = utils.strftime(query_date, format_str=format_str)
    report_query = f"SELECT {','.join(selected_fields)} FROM {resource_name} WHERE segments.date = '{query_date}' {build_parameters()}"

    return report_query


def generate_hash(record, metadata):
    metadata = singer.metadata.to_map(metadata)
    fields_to_hash = []
    for key, val in record.items():
        if metadata[("properties", key)]["behavior"] != "METRIC":
            fields_to_hash.append((key, val))

    hash_source_data = sorted(fields_to_hash, key=lambda x: x[0])
    hash_bytes = json.dumps(hash_source_data).encode("utf-8")
    return hashlib.sha256(hash_bytes).hexdigest()


retryable_errors = [
    "QuotaError.RESOURCE_EXHAUSTED",
    "QuotaError.RESOURCE_TEMPORARILY_EXHAUSTED",
    "InternalError.INTERNAL_ERROR",
    "InternalError.TRANSIENT_ERROR",
    "InternalError.DEADLINE_EXCEEDED",
]


def should_give_up(ex):
    if isinstance(ex, AttributeError):
        if str(ex) == "'NoneType' object has no attribute 'Call'":
            LOGGER.info('Retrying request due to AttributeError')
            return False
        return True

    for googleads_error in ex.failure.errors:
        quota_error = str(googleads_error.error_code.quota_error)
        internal_error = str(googleads_error.error_code.internal_error)
        for err in [quota_error, internal_error]:
            if err in retryable_errors:
                LOGGER.info(f'Retrying request due to {err}')
                return False
    return True


def on_giveup_func(err):
    """This function lets us know that backoff ran, but it does not print
    Google's verbose message and stack trace"""
    LOGGER.warning("Giving up make_request after %s tries", err.get("tries"))


@backoff.on_exception(backoff.expo,
                      (GoogleAdsException,
                       AttributeError),
                      max_tries=5,
                      jitter=None,
                      giveup=should_give_up,
                      on_giveup=on_giveup_func,
                      logger=None)
def make_request(gas, query, customer_id):
    response = gas.search(query=query, customer_id=customer_id)
    return response


def google_message_to_json(message):
    """
    The proto field name for `type` is `type_` which will
    get stripped by the Transformer. So we replace all
    instances of the key `"type_"` before `json.loads`ing it
    """

    json_string = MessageToJson(message, preserving_proto_field_name=True)
    json_string = json_string.replace('"type_":', '"type":')
    return json.loads(json_string)


class BaseStream:  # pylint: disable=too-many-instance-attributes

    def __init__(self, fields, google_ads_resource_names, resource_schema, primary_keys):
        self.fields = fields
        self.google_ads_resource_names = google_ads_resource_names
        self.primary_keys = primary_keys

        self.extract_field_information(resource_schema)

        self.create_full_schema(resource_schema)
        self.set_stream_schema()
        self.format_field_names()

        self.build_stream_metadata()


    def extract_field_information(self, resource_schema):
        self.field_exclusions = defaultdict(set)
        self.schema = {}
        self.behavior = {}

        for resource_name in self.google_ads_resource_names:

            # field_exclusions step
            fields = resource_schema[resource_name]["fields"]
            for field_name, field in fields.items():
                if field_name in self.fields:
                    self.field_exclusions[field_name].update(
                        field["incompatible_fields"]
                    )

                    self.schema[field_name] = field["field_details"]["json_schema"]

                    self.behavior[field_name] = field["field_details"]["category"]

            self.add_extra_fields(resource_schema)
        self.field_exclusions = {k: list(v) for k, v in self.field_exclusions.items()}

    def add_extra_fields(self, resource_schema):
        """This function should add fields to `field_exclusions`, `schema`, and
        `behavior` that are not covered by Google's resource_schema
        """

    def create_full_schema(self, resource_schema):
        google_ads_name = self.google_ads_resource_names[0]
        self.resource_object = resource_schema[google_ads_name]
        self.resource_fields = self.resource_object["fields"]
        self.full_schema = create_nested_resource_schema(resource_schema, self.resource_fields)

    def set_stream_schema(self):
        google_ads_name = self.google_ads_resource_names[0]
        self.stream_schema = self.full_schema["properties"][google_ads_name]

    def format_field_names(self):
        """This function does two things:
        1. Appends a `resource_name` to an id field if it is the id of an attributed resource
        2. Lifts subfields of `ad_group_ad.ad` into `ad_group_ad`
        """
        for resource_name, schema in self.full_schema["properties"].items():
            # ads stream is special since all of the ad fields are nested under ad_group_ad.ad
            # we need to bump the fields up a level so they are selectable
            if resource_name == "ad_group_ad":
                for ad_field_name, ad_field_schema in self.full_schema["properties"]["ad_group_ad"]["properties"]["ad"]["properties"].items():
                    self.stream_schema["properties"][ad_field_name] = ad_field_schema
                self.stream_schema["properties"].pop("ad")

            if (
                resource_name not in {"metrics", "segments"}
                and resource_name not in self.google_ads_resource_names
            ):
                self.stream_schema["properties"][resource_name + "_id"] = schema["properties"]["id"]

    def build_stream_metadata(self):
        self.stream_metadata = {
            (): {
                "inclusion": "available",
                "forced-replication-method": "FULL_TABLE",
                "table-key-properties": self.primary_keys,
            }
        }

        for field, props in self.resource_fields.items():
            resource_matches = field.startswith(self.resource_object["name"] + ".")
            is_id_field = field.endswith(".id")

            if is_id_field or (props["field_details"]["category"] == "ATTRIBUTE" and resource_matches):
                # Transform the field name to match the schema
                # Special case for ads since they are nested under ad_group_ad and
                # we have to bump them up a level
                if field.startswith("ad_group_ad.ad."):
                    field = field.split(".")[2]
                else:
                    if resource_matches:
                        field = field.split(".")[1]
                    elif is_id_field:
                        field = field.replace(".", "_")

                if ("properties", field) not in self.stream_metadata:
                    # Base metadata for every field
                    self.stream_metadata[("properties", field)] = {
                        "fieldExclusions": props["incompatible_fields"],
                        "behavior": props["field_details"]["category"],
                    }

                    # Add inclusion metadata
                    # Foreign keys are automatically included and they are all id fields
                    if field in self.primary_keys or field in {'customer_id', 'ad_group_id', 'campaign_id'}:
                        inclusion = "automatic"
                    elif props["field_details"]["selectable"]:
                        inclusion = "available"
                    else:
                        # inclusion = "unsupported"
                        continue
                    self.stream_metadata[("properties", field)]["inclusion"] = inclusion

                # Save the full field name for sync code to use
                full_name = props["field_details"]["name"]
                if "tap-google-ads.api-field-names" not in self.stream_metadata[("properties", field)]:
                    self.stream_metadata[("properties", field)]["tap-google-ads.api-field-names"] = []

                if props["field_details"]["selectable"]:
                    self.stream_metadata[("properties", field)]["tap-google-ads.api-field-names"].append(full_name)

    def transform_keys(self, obj):
        """This function does a few things with Google's response for sync queries:
        1) checks an object's fields to see if they're for the current resource
        2) if they are, keep the fields in transformed_obj with no modifications
        3) if they are not, append a foreign key to the transformed_obj using the id value
        4) if the resource is ad_group_ad, pops ad fields up to the ad_group_ad level

        We've seen API responses where Google returns `type_` when the
        field we ask for is `type`, so we transfrom the key-value pair
        `"type_": X` to `"type": X`
        """
        target_resource_name = self.google_ads_resource_names[0]
        transformed_obj = {}

        for resource_name, value in obj.items():
            resource_matches = target_resource_name == resource_name

            if resource_matches:
                transformed_obj.update(value)
            else:
                transformed_obj[f"{resource_name}_id"] = value["id"]

            if resource_name == "ad_group_ad":
                transformed_obj.update(value["ad"])
                transformed_obj.pop("ad")

        if "type_" in transformed_obj:
            transformed_obj["type"] = transformed_obj.pop("type_")

        return transformed_obj

    def sync(self, sdk_client, customer, stream, config, state): # pylint: disable=unused-argument
        gas = sdk_client.get_service("GoogleAdsService", version=API_VERSION)
        resource_name = self.google_ads_resource_names[0]
        stream_name = stream["stream"]
        stream_mdata = stream["metadata"]
        selected_fields = get_selected_fields(stream_mdata)
        state = singer.set_currently_syncing(state, [stream_name, customer["customerId"]])
        singer.write_state(state)

        query = create_core_stream_query(resource_name, selected_fields)
        try:
            response = make_request(gas, query, customer["customerId"])
        except GoogleAdsException as err:
            LOGGER.warning("Failed query: %s", query)
            raise err

        with Transformer() as transformer:
            # Pages are fetched automatically while iterating through the response
            for message in response:
                json_message = google_message_to_json(message)
                transformed_obj = self.transform_keys(json_message)
                record = transformer.transform(transformed_obj, stream["schema"], singer.metadata.to_map(stream_mdata))

                singer.write_record(stream_name, record)


def get_query_date(start_date, bookmark, conversion_window_date):
    """Return a date within the conversion window and after start date

    All inputs are datetime strings.
    NOTE: `bookmark` may be None"""
    if not bookmark:
        return singer.utils.strptime_to_utc(start_date)
    else:
        query_date = min(bookmark, max(start_date, conversion_window_date))
        return singer.utils.strptime_to_utc(query_date)


class ReportStream(BaseStream):
    def create_full_schema(self, resource_schema):
        google_ads_name = self.google_ads_resource_names[0]
        self.resource_object = resource_schema[google_ads_name]
        self.resource_fields = self.resource_object["fields"]
        self.full_schema = create_nested_resource_schema(resource_schema, self.fields)

    def set_stream_schema(self):
        self.stream_schema = {
            "type": ["null", "object"],
            "is_report": True,
            "properties": {
                "_sdc_record_hash": {"type": "string"}
            },
        }

    def format_field_names(self):
        """This function does two things right now:
        1. Appends a `resource_name` to a field name if the field is in an attributed resource
        2. Lifts subfields of `ad_group_ad.ad` into `ad_group_ad`
        """
        for resource_name, schema in self.full_schema["properties"].items():
            for field_name, data_type in schema["properties"].items():
                # Move ad_group_ad.ad.x fields up a level in the schema (ad_group_ad.ad.x -> ad_group_ad.x)
                if resource_name == "ad_group_ad" and field_name == "ad":
                    for ad_field_name, ad_field_schema in data_type["properties"].items():
                        self.stream_schema["properties"][ad_field_name] = ad_field_schema
                elif resource_name not in {"metrics", "segments"}:
                    self.stream_schema["properties"][f"{resource_name}_{field_name}"] = data_type
                else:
                    self.stream_schema["properties"][field_name] = data_type

    def build_stream_metadata(self):
        self.stream_metadata = {
            (): {
                "inclusion": "available",
                "table-key-properties": ["_sdc_record_hash"],
                "forced-replication-method": "INCREMENTAL",
                "valid-replication-keys": ["date"]
            },
            ("properties", "_sdc_record_hash"): {
                "inclusion": "automatic",
                "behavior": "PRIMARY KEY"
            },
        }
        for report_field in self.fields:
            # Transform the field name to match the schema
            is_metric_or_segment = report_field.startswith("metrics.") or report_field.startswith("segments.")
            # Transform ad_group_ad.ad.x fields to just x to reflect ad_group_ads schema
            if report_field.startswith("ad_group_ad.ad."):
                transformed_field_name = report_field.split(".")[2]
            elif not is_metric_or_segment:
                transformed_field_name = "_".join(report_field.split(".")[:2])
            else:
                transformed_field_name = report_field.split(".")[1]
            # TODO: Maybe refactor this
            # metadata_key = ("properties", transformed_field_name)
            # Base metadata for every field
            if ("properties", transformed_field_name) not in self.stream_metadata:
                self.stream_metadata[("properties", transformed_field_name)] = {
                    "fieldExclusions": [],
                    "behavior": self.behavior[report_field],
                }

                # Transform field exclusion names so they match the schema
                for field_name in self.field_exclusions[report_field]:
                    is_metric_or_segment = field_name.startswith("metrics.") or field_name.startswith("segments.")
                    if not is_metric_or_segment:
                        new_field_name = field_name.replace(".", "_")
                    else:
                        new_field_name = field_name.split(".")[1]

                    self.stream_metadata[("properties", transformed_field_name)]["fieldExclusions"].append(new_field_name)

            # Add inclusion metadata
            if self.behavior[report_field]:
                inclusion = "available"
                if report_field == "segments.date":
                    inclusion = "automatic"
            else:
                inclusion = "unsupported"
            self.stream_metadata[("properties", transformed_field_name)]["inclusion"] = inclusion

            # Save the full field name for sync code to use
            if "tap-google-ads.api-field-names" not in self.stream_metadata[("properties", transformed_field_name)]:
                self.stream_metadata[("properties", transformed_field_name)]["tap-google-ads.api-field-names"] = []

            self.stream_metadata[("properties", transformed_field_name)]["tap-google-ads.api-field-names"].append(report_field)

    def transform_keys(self, obj):
        transformed_obj = {}

        for resource_name, value in obj.items():
            if resource_name in {"metrics", "segments"}:
                transformed_obj.update(value)
            elif resource_name == "ad_group_ad":
                for key, sub_value in value.items():
                    if key == 'ad':
                        transformed_obj.update(sub_value)
                    else:
                        transformed_obj.update({f"{resource_name}_{key}": sub_value})
            else:
                # value = {"a": 1, "b":2}
                # turns into
                # {"resource_a": 1, "resource_b": 2}
                transformed_obj.update(
                    {f"{resource_name}_{key}": sub_value
                     for key, sub_value in value.items()}
                )

        return transformed_obj

    def sync(self, sdk_client, customer, stream, config, state):
        gas = sdk_client.get_service("GoogleAdsService", version=API_VERSION)
        resource_name = self.google_ads_resource_names[0]
        stream_name = stream["stream"]
        stream_mdata = stream["metadata"]
        selected_fields = get_selected_fields(stream_mdata)
        replication_key = "date"
        state = singer.set_currently_syncing(state, [stream_name, customer["customerId"]])
        singer.write_state(state)

        conversion_window = timedelta(
            days=get_conversion_window(config)
        )
        conversion_window_date = utils.now().replace(hour=0, minute=0, second=0, microsecond=0) - conversion_window

        bookmark_object = singer.get_bookmark(state, stream["tap_stream_id"], customer["customerId"], default={})

        bookmark_value = bookmark_object.get(replication_key)

        query_date = get_query_date(
            start_date=config["start_date"],
            bookmark=bookmark_value,
            conversion_window_date=singer.utils.strftime(conversion_window_date)
        )

        end_date = config.get("end_date")
        if end_date:
            end_date = utils.strptime_to_utc(end_date)
        else:
            end_date = utils.now()

        if stream_name in REPORTS_WITH_90_DAY_MAX:
            cutoff = end_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=90)
            query_date = max(query_date, cutoff)
            if query_date == cutoff:
                LOGGER.info(f"Stream: {stream_name} supports only 90 days of data. Setting query date to {utils.strftime(query_date, '%Y-%m-%d')}.")

        if selected_fields == {'segments.date'}:
            raise Exception(f"Selected fields is currently limited to {', '.join(selected_fields)}. Please select at least one attribute and metric in order to replicate {stream_name}.")

        while query_date <= end_date:
            query = create_report_query(resource_name, selected_fields, query_date)
            LOGGER.info(f"Requesting {stream_name} data for {utils.strftime(query_date, '%Y-%m-%d')}.")

            try:
                response = make_request(gas, query, customer["customerId"])
            except GoogleAdsException as err:
                LOGGER.warning("Failed query: %s", query)
                LOGGER.critical(str(err.failure.errors[0].message))
                raise RuntimeError from None


            with Transformer() as transformer:
                # Pages are fetched automatically while iterating through the response
                for message in response:
                    json_message = google_message_to_json(message)
                    transformed_obj = self.transform_keys(json_message)
                    record = transformer.transform(transformed_obj, stream["schema"])
                    record["_sdc_record_hash"] = generate_hash(record, stream_mdata)

                    singer.write_record(stream_name, record)

            new_bookmark_value = {replication_key: utils.strftime(query_date)}
            singer.write_bookmark(state, stream["tap_stream_id"], customer["customerId"], new_bookmark_value)

            singer.write_state(state)

            query_date += timedelta(days=1)


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
        "labels": BaseStream(
            report_definitions.LABEL_FIELDS,
            ["label"],
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
        "ad_group_performance_report": ReportStream(
            report_definitions.AD_GROUP_PERFORMANCE_REPORT_FIELDS,
            ["ad_group"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "ad_group_audience_performance_report": ReportStream(
            report_definitions.AD_GROUP_AUDIENCE_PERFORMANCE_REPORT_FIELDS,
            ["ad_group_audience_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
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
        "campaign_performance_report": ReportStream(
            report_definitions.CAMPAIGN_PERFORMANCE_REPORT_FIELDS,
            ["campaign"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "campaign_audience_performance_report": ReportStream(
            report_definitions.CAMPAIGN_AUDIENCE_PERFORMANCE_REPORT_FIELDS,
            ["campaign_audience_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "click_performance_report": ReportStream(
            report_definitions.CLICK_PERFORMANCE_REPORT_FIELDS,
            ["click_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
        "display_keyword_performance_report": ReportStream(
            report_definitions.DISPLAY_KEYWORD_PERFORMANCE_REPORT_FIELDS,
            ["display_keyword_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
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
            ["_sdc_record_hash"],
        ),
        "gender_performance_report": ReportStream(
            report_definitions.GENDER_PERFORMANCE_REPORT_FIELDS,
            ["gender_view"],
            resource_schema,
            ["_sdc_record_hash"],
        ),
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
            ["_sdc_record_hash"],
        ),
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
        "user_location_performance_report": ReportStream(
            report_definitions.USER_LOCATION_PERFORMANCE_REPORT_FIELDS,
            ["user_location_view"],
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
