import json

import singer

from tap_google_ads.client import create_sdk_client
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

    for catalog_entry in selected_streams:
        stream_name = catalog_entry["stream"]
        mdata_map = singer.metadata.to_map(catalog_entry["metadata"])

        primary_key = mdata_map[()].get("table-key-properties", [])
        singer.messages.write_schema(stream_name, catalog_entry["schema"], primary_key)

        for customer in customers:
            sdk_client = create_sdk_client(config, customer["loginCustomerId"])

            LOGGER.info(f"Syncing {stream_name} for customer Id {customer['customerId']}.")

            if core_streams.get(stream_name):
                stream_obj = core_streams[stream_name]
            else:
                stream_obj = report_streams[stream_name]

            stream_obj.sync(sdk_client, customer, catalog_entry, config, state)
