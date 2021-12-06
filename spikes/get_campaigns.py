"""Gets the account hierarchy of the given MCC and login customer ID.
If you don't specify manager ID and login customer ID, the example will instead
print the hierarchies of all accessible customer accounts for your
authenticated Google account. Note that if the list of accessible customers for
your authenticated Google account includes accounts within the same hierarchy,
this example will retrieve and print the overlapping portions of the hierarchy
for each accessible customer.
"""

import argparse
import sys
import json

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf.json_format import MessageToDict
from google.protobuf.json_format import MessageToJson

def main(client, login_customer_id=None):
    """Gets the account hierarchy of the given MCC and login customer ID.
    Args:
      client: The Google Ads client.
      login_customer_id: Optional manager account ID. If none provided, this
      method will instead list the accounts accessible from the
      authenticated Google Ads account.
    """

    # Gets instances of the GoogleAdsService and CustomerService clients.
    googleads_service = client.get_service("GoogleAdsService")
    customer_service = client.get_service("CustomerService")
    campaign_service = client.get_service("CampaignService")

    # A collection of customer IDs to handle.
    seed_customer_ids = [4224806558]
    #Manager
    #login_customer_id = '9837412151'
    #Non Manager
    #login_customer_id = '4224806558'

    
    # Creates a query that retrieves all child accounts of the manager
    # specified in search calls below.
    query = """
        SELECT
          campaign.resource_name
        FROM campaign
        """

    for seed_customer_id in seed_customer_ids:
        # Performs a breadth-first search to build a Dictionary that maps
        # managers to their child accounts (customerIdsToChildAccounts).
        unprocessed_customer_ids = [seed_customer_id]
        customer_ids_to_child_accounts = dict()
        root_customer_client = None

        while unprocessed_customer_ids:
            customer_id = int(unprocessed_customer_ids.pop(0))
            response = googleads_service.search(
                customer_id=str(customer_id), query=query
            )
            #all_campaigns = json.load(response)
            all_campaigns = []
            
            for googleads_row in response:
                all_campaigns.append(googleads_row.campaign.resource_name)

            for resource_name in all_campaigns:
                response2 = campaign_service.get_campaign(resource_name=resource_name)
                json_response = MessageToJson(response2)
                print(json_response)
                break
                
if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage(version="v9")

    parser = argparse.ArgumentParser(
        description="This example gets the account hierarchy of the specified "
        "manager account and login customer ID."
    )
    # The following argument(s) should be provided to run the example.
    parser.add_argument(
        "-l",
        "--login_customer_id",
        "--manager_customer_id",
        type=str,
        required=False,
        help="Optional manager "
        "account ID. If none provided, the example will "
        "instead list the accounts accessible from the "
        "authenticated Google Ads account.",
    )
    args = parser.parse_args()
    try:
        main(googleads_client, args.login_customer_id)
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)
