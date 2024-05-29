import os
from google.ads.googleads.client import GoogleAdsClient
from google.oauth2.service_account import Credentials as ServiceAccountCreds


def get_service_account_credentials():
    service_account_info = os.getenv["SERVICE_ACCOUNT_INFO_STRING"]
    return ServiceAccountCreds.from_service_account_info(service_account_info)


class GoogleAdsClientServiceAccount(GoogleAdsClient):
    @classmethod
    def _get_client_kwargs(cls, config_data):
        """Converts configuration dict into kwargs required by the client.

        Args:
            config_data: a dict containing client configuration.

        Returns:
            A dict containing kwargs that will be provided to the
            GoogleAdsClient initializer.

        Raises:
            ValueError: If the configuration lacks a required field.
        """
        return {
            "credentials": get_service_account_credentials(config_data),
            "developer_token": config_data.get("developer_token"),
            "endpoint": config_data.get("endpoint"),
            "login_customer_id": config_data.get("login_customer_id"),
            "logging_config": config_data.get("logging"),
            "linked_customer_id": config_data.get("linked_customer_id"),
            "http_proxy": config_data.get("http_proxy"),
            "use_proto_plus": config_data.get("use_proto_plus"),
            "use_cloud_org_for_api_access": config_data.get(
                "use_cloud_org_for_api_access"
            ),
        }


def create_sdk_client(config, login_customer_id=None):
    CONFIG = {
        "use_proto_plus": False,
        "developer_token": config["developer_token"],
        "client_id": config["oauth_client_id"],
        "client_secret": config["oauth_client_secret"],
        "refresh_token": config["refresh_token"],
    }

    if login_customer_id:
        CONFIG["login_customer_id"] = login_customer_id

    sdk_client = GoogleAdsClient.load_from_dict(CONFIG)
    return sdk_client
