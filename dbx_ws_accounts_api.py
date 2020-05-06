# Interface for Databricks E2 Accounts API
# Majorly to create different objects related to a E2 workspace, and any pre or post processings

import time

from databricks_cli.sdk import ApiClient
from databricks_cli.accounts import AccountsApi

class DatabricksWSAccountsAPI(object):

    def __init__(self, common_params):
        dbcli_apiclient = ApiClient(common_params["api_user"], password=common_params["api_password"],
                                host='https://accounts.cloud.databricks.com', 
                                verify=True, command_name='Python Dev')
        self.accounts_api_client = AccountsApi(dbcli_apiclient)

    # Create credentials object for a E2 workspace
    def _create_credentials(self, common_params, other_input_data):
        print("Creating the Databricks workspace credentials")
        credentials_id = None
        credentials_request = {
            "credentials_name": common_params["credentials_name"],
            "aws_credentials": {
                "sts_role": {
                    "role_arn": other_input_data["iam_role_arn"]
                }
            }
        }
        credentials_resp = self.accounts_api_client.create_credentials(
                                common_params["databricks_workspace_account_id"], credentials_request)
        credentials_id = credentials_resp['credentials_id']
        print("Credentials id is {}".format(credentials_id))

        if credentials_id is None:
            print("Exiting the script as credentials object was not created successfully")
            exit(1)
        return credentials_id

    # Create storage config object for a E2 workspace
    def _create_storage_config(self, common_params, other_input_data):
        print("Creating the Databricks workspace storage config")
        storage_config_id = None
        storage_config_request = {
            "storage_configuration_name": common_params["storage_config_name"],
            "root_bucket_info": {
                "bucket_name": other_input_data["s3_bucket_name_final"]
            }
        }
        storage_config_resp = self.accounts_api_client.create_storage_config(
                                    common_params["databricks_workspace_account_id"], storage_config_request)
        storage_config_id = storage_config_resp['storage_configuration_id']
        print("Storage config id is {}".format(storage_config_id))

        if storage_config_id is None:
            print("Exiting the script as storage config object was not created successfully")
            exit(1)
        return storage_config_id

    # Create network object for a E2 workspace
    def _create_network(self, common_params, other_input_data):
        print("Creating the Databricks workspace network")
        network_id = None
        network_request = {
            "network_name": common_params["network_name"],
            "vpc_id": common_params["vpc_id"],
            "subnet_ids": [other_input_data["subnet1_id"], other_input_data["subnet2_id"]],
            "security_group_ids": [other_input_data["security_group_id"]]
        }
        network_resp = self.accounts_api_client.create_network(
                            common_params["databricks_workspace_account_id"], network_request)
        network_id = network_resp['network_id']
        print("Network id is {}".format(network_id))

        if network_id is None:
            print("Exiting the script as network object was not created successfully")
            exit(1)
        return network_id

    # Create customer managed key object for a E2 workspace
    def _create_customer_managed_key(self, common_params, other_input_data):
        print("Creating the Databricks workspace customer managed key")
        customer_managed_key_id = None
        customer_managed_key_request = {
            "aws_key_info": {
                "key_arn": other_input_data["kms_key_arn"],
                "key_alias": other_input_data["kms_key_alias"],
                "key_region": common_params["region_name"]
            }
        }
        customer_managed_key_resp = self.accounts_api_client.create_customer_managed_key(
                                        common_params["databricks_workspace_account_id"], customer_managed_key_request)
        customer_managed_key_id = customer_managed_key_resp['customer_managed_key_id']
        print("Customer managed key id is {}".format(customer_managed_key_id))

        if customer_managed_key_id is None:
            print("Exiting the script as customer managed key object was not created successfully")
            exit(1)
        return customer_managed_key_id

    # Create the E2 workspace using previously created object references
    def _create_workspace(self, common_params, other_input_data):
        print("Creating the Databricks workspace")
        workspace_id = None
        workspace_request = {
            "workspace_name": common_params["workspace_name"],
            "deployment_name": common_params["deployment_cname"],
            "aws_region": common_params["region_name"],
            "credentials_id": other_input_data["credentials_id"],
            "network_id": other_input_data["network_id"],
            "storage_configuration_id": other_input_data["storage_config_id"],
            "customer_managed_key_id": other_input_data["customer_managed_key_id"],
            "is_no_public_ip_enabled": True
        }
        workspace_resp = self.accounts_api_client.create_workspace(common_params["databricks_workspace_account_id"], workspace_request)
        workspace_id = workspace_resp['workspace_id']
        print("Workspace id is {}".format(workspace_id))

        if workspace_id is None:
            print("Exiting the script as workspace request was not posted successfully")
            exit(1)
        return workspace_id

    # Check if the workspace has been provisioned successfully
    def _check_workspace_provisioning(self, common_params, other_input_data):
        workspace_prov_status = 'PROVISIONING'
        while workspace_prov_status == 'PROVISIONING':
            time.sleep(5)
            workspace_prov_resp = self.accounts_api_client.get_workspace(
                                        common_params["databricks_workspace_account_id"], other_input_data["workspace_id"])
            workspace_prov_status = workspace_prov_resp['workspace_status']
        return workspace_prov_status
        