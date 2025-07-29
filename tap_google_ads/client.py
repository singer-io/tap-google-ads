from google.ads.googleads.client import GoogleAdsClient


def create_sdk_client(config, login_customer_id=None, service_account_auth_file=False, service_account_auth_string=False):
    if service_account_auth_file:
        CONFIG = {
            "use_proto_plus": config["use_proto_plus"],
            "developer_token": config["developer_token"],
            "impersonated_email": config["impersonated_email"],
            "json_key_string": config["json_key_file"],
        }
    elif service_account_auth_string:
        CONFIG = {
            "use_proto_plus": config["use_proto_plus"],
            "developer_token": config["developer_token"],
            "impersonated_email": config["impersonated_email"],
            "json_key_string": config["json_key_string"],
        }
    else:
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
