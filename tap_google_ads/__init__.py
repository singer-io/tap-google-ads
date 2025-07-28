#!/usr/bin/env python3
import logging
import singer
import datetime
from singer import utils
from tap_google_ads.discover import create_resource_schema
from tap_google_ads.discover import do_discover
from tap_google_ads.sync import do_sync


LOGGER = singer.get_logger()


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "oauth_client_id",
    "oauth_client_secret",
    "refresh_token",
    "customer_ids",
    "developer_token",
]


def fail_connection(state):
    """
    Fail the connection once every 7 days to ensure customers are aware of the version deprecation.
    """
    today = datetime.datetime.now(datetime.timezone.utc)

    # Get the last triggered date from state
    last_triggered_raw = state.get('last_exception_triggered')
    last_triggered_date = utils.strptime_with_tz(last_triggered_raw) if last_triggered_raw else None
    iso_today = today.strftime('%Y-%m-%dT%H:%M:%SZ')

    if last_triggered_date and (today - last_triggered_date <= datetime.timedelta(days=7)):
        return

    state['last_exception_triggered'] = iso_today
    singer.write_state(state)

    if not last_triggered_date:
        return
    else:
        # Raise exception to trigger Stitch notification
        raise Exception(
            "Current version 1 of this integration is available till 2025-08-20 only."
            "Please upgrade to version 2 to ensure continued extraction."
        )


def main_impl():
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
        fail_connection(state)
        LOGGER.info("Sync Completed")
    else:
        LOGGER.info("No properties were selected")

def main():

    google_logger = logging.getLogger("google")
    google_logger.setLevel(level=logging.CRITICAL)

    try:
        main_impl()
    except Exception as e:
        for line in str(e).splitlines():
            LOGGER.critical(line)
        raise e


if __name__ == "__main__":
    main()
