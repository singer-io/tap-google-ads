#!/usr/bin/env python3
import json
import os
import sys

import singer
from singer import utils
from singer import metrics
from singer import bookmarks
from singer import metadata
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer)

LOGGER = singer.get_logger()
CORE_ENDPOINT_MAPPINGS =    {"campaigns": {'primary_keys': ["id"],
                                           'service_name': 'CampaignService'},
                             "ad_groups": {'primary_keys': ["id"],
                                           'service_name': 'AdGroupService'},
                             "ads":       {'primary_keys': ["adGroupId"],
                                           'service_name': 'AdGroupAdService'},
                             "accounts":  {'primary_keys': ["customerId"],
                                           'service_name': 'ManagedCustomerService'}}

def create_field_metadata(stream, schema):
    primary_key = CORE_ENDPOINT_MAPPINGS[stream]['primary_keys']

    mdata = {}
    mdata = metadata.write(mdata, (), 'inclusion', 'available')
    mdata = metadata.write(mdata, (), 'table-key-properties', primary_key)

    for field in schema['properties']:
        breadcrumb = ('properties', str(field))
        mdata = metadata.write(mdata, breadcrumb, 'inclusion', 'available')

    mdata = metadata.write(mdata, ('properties', primary_key[0]), 'inclusion', 'automatic')
    mdata = metadata.to_list(mdata)

    return mdata

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

def load_metadata(entity):
   return utils.load_json(get_abs_path("metadata/{}.json".format(entity)))

def do_discover_core_endpoints():
    streams = []
    LOGGER.info("Starting core discovery")
    for stream_name in CORE_ENDPOINT_MAPPINGS:
        LOGGER.info('Loading schema for %s', stream_name)
        schema = load_schema(stream_name)
        md = create_field_metadata(stream_name, schema)
        streams.append({'stream': stream_name,
                        'tap_stream_id': stream_name,
                        'schema': schema,
                        'metadata': md})
    LOGGER.info("Core discovery complete")
    return streams

def do_discover(customer_ids):
    # sdk_client = create_sdk_client(customer_ids[0])
    core_streams = do_discover_core_endpoints()
    # report_streams = do_discover_reports(sdk_client)
    streams = []
    streams.extend(core_streams)
    # streams.extend(report_streams)
    json.dump({"streams": streams}, sys.stdout, indent=2)

def create_sdk_client(customer_id):
    oauth2_client = oauth2.GoogleRefreshTokenClient(
        CONFIG['oauth_client_id'], \
        CONFIG['oauth_client_secret'], \
        CONFIG['refresh_token'])

    sdk_client = adwords.AdWordsClient(CONFIG['developer_token'], \
                                 oauth2_client, user_agent=CONFIG['user_agent'], \
                                 client_customer_id=customer_id)
    return sdk_client

def main():
    do_discover(1234567890)

if __name__ == "__main__":
    main()
