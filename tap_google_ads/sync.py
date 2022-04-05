import json

import singer

from tap_google_ads.client import create_sdk_client
from tap_google_ads.streams import initialize_core_streams, initialize_reports

LOGGER = singer.get_logger()


def get_currently_syncing(state):
    currently_syncing = state.get("currently_syncing")

    if not currently_syncing:
        currently_syncing = (None, None)

    resuming_stream, resuming_customer = currently_syncing
    return resuming_stream, resuming_customer


def sort_customers(customers):
    return sorted(customers, key=lambda x: x["customerId"])

def sort_selected_streams(sort_list):
    return sorted(sort_list, key=lambda x: x["tap_stream_id"])


def shuffle(shuffle_list, shuffle_key, current_value, sort_function):
    """Return `shuffle_list` with `current_value` at the front of the list

    In the scenario where `current_value` is not in `shuffle_list`:
    - Assume that we have a consistent ordering to `shuffle_list`
    - Insert the `current_value` into `shuffle_list`
    - Sort the new list
    - Do the normal logic to shuffle the list
    - Return the new shuffled list without the `current_value` we inserted"""

    fallback = False
    if current_value not in [item[shuffle_key] for item in shuffle_list]:
        fallback = True
        shuffle_list.append({shuffle_key: current_value})
        shuffle_list = sort_function(shuffle_list)

    matching_index = 0
    for i, key in enumerate(shuffle_list):
        if key[shuffle_key] == current_value:
            matching_index = i
            break
    top_half = shuffle_list[matching_index:]
    bottom_half = shuffle_list[:matching_index]

    if fallback:
        return top_half[1:] + bottom_half

    return top_half + bottom_half


def do_sync(config, catalog, resource_schema, state, schemaless):
    # QA ADDED WORKAROUND [START]
    try:
        customers = json.loads(config["login_customer_ids"])
    except TypeError:  # falling back to raw value
        customers = config["login_customer_ids"]
    # QA ADDED WORKAROUND [END]
    customers = sort_customers(customers)

    selected_streams = [
        stream
        for stream in catalog["streams"]
        if singer.metadata.to_map(stream["metadata"])[()].get("selected")
    ]
    selected_streams = sort_selected_streams(selected_streams)

    core_streams = initialize_core_streams(resource_schema)
    report_streams = initialize_reports(resource_schema)
    resuming_stream, resuming_customer = get_currently_syncing(state)

    if resuming_stream:
        selected_streams = shuffle(
            selected_streams,
            "tap_stream_id",
            resuming_stream,
            sort_function=sort_selected_streams
        )

    if resuming_customer:
        customers = shuffle(
            customers,
            "customerId",
            resuming_customer,
            sort_function=sort_customers
        )

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

            stream_obj.sync(sdk_client, customer, catalog_entry, config, state, schemaless)

    state.pop("currently_syncing", None)
    singer.write_state(state)
