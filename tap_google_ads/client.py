from google.ads.googleads.client import GoogleAdsClient

from tap_google_ads.streams import API_VERSION


def create_sdk_client(config, login_customer_id=None):
    CONFIG = {
        "use_proto_plus": False,
        "developer_token": config["developer_token"],
        "client_id": config["oauth_client_id"],
        "client_secret": config["oauth_client_secret"],
        "refresh_token": config["refresh_token"],
    }

    if login_customer_id:
        CONFIG["customer_id"] = login_customer_id

    sdk_client = GoogleAdsClient.load_from_dict(CONFIG, version=API_VERSION)
    return sdk_client
