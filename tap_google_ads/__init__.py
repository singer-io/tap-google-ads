#!/usr/bin/env python3
import singer
from singer import utils

from tap_google_ads.client import REQUIRED_CONFIG_KEYS
from tap_google_ads.discover import create_resource_schema
from tap_google_ads.discover import do_discover
from tap_google_ads.sync import do_sync


LOGGER = singer.get_logger()


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    resource_schema = create_resource_schema(args.config)
    state = {}
    
    if args.state:
        state.update(args.state)
    if args.discover:
        do_discover(resource_schema)
        LOGGER.info("Discovery complete")
    elif args.catalog:
        do_sync(args.config, args.catalog.to_dict(), resource_schema, state)
        LOGGER.info("Sync Completed")
    else:
        LOGGER.info("No properties were selected")


if __name__ == "__main__":
    main()
