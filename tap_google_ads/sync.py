import json

import singer
from singer import metadata

from tap_google_ads.client import create_sdk_client, REQUIRED_CONFIG_KEYS
from tap_google_ads.streams import initialize_core_streams, initialize_reports


LOGGER = singer.get_logger()


def do_sync(config, catalog, resource_schema, state):
    # QA ADDED WORKAROUND [START]
    try:
        customers = json.loads(config["login_customer_ids"])
    except TypeError:  # falling back to raw value
        customers = config["login_customer_ids"]
    # QA ADDED WORKAROUND [END]

    selected_streams = [
        stream
        for stream in catalog["streams"]
        if singer.metadata.to_map(stream["metadata"])[()].get("selected")
    ]

    core_streams = initialize_core_streams(resource_schema)
    report_streams = initialize_reports(resource_schema)

    for customer in customers:
        LOGGER.info(f"Syncing customer Id {customer['customerId']} ...")
        sdk_client = create_sdk_client(config, customer["loginCustomerId"])
        for catalog_entry in selected_streams:
            stream_name = catalog_entry["stream"]
            mdata_map = singer.metadata.to_map(catalog_entry["metadata"])

            primary_key = (
                mdata_map[()].get("table-key-properties", [])
            )
            singer.messages.write_schema(stream_name, catalog_entry["schema"], primary_key)

            LOGGER.info(f"Syncing {stream_name} for customer Id {customer['customerId']}.")
            if stream_name in core_streams:
                stream_obj = core_streams[stream_name]
                stream_obj.sync_core_streams(sdk_client, customer, catalog_entry)
            else:
                # syncing report
                stream_obj = report_streams[stream_name]
                stream_obj.sync_report_streams(sdk_client, customer, catalog_entry, config, state)
