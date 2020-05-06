# This is a quick and dirty script to do full end-to-end AWS infra and Databricks E2 workspace
# provisioning. To do something similar in CI/CD pipelines, I would suggest to modularize
# and externalize the config further.

from json import loads as json_loads

from dbx_ws_utils import DatabricksWSProvisioningUtils
from dbx_ws_stack_processor import DatabricksWSStackProcessor
from dbx_ws_accounts_api import DatabricksWSAccountsAPI

from databricks_cli.sdk import ApiClient
from databricks_cli.accounts import AccountsApi

# Get the required master parameters to be used below
with open('./common_params.json') as parameter_fileobj:
    parameter_str = parameter_fileobj.read()
common_params = json_loads(parameter_str)

# Create object to invoke utility methods - mostly for cloudformation related interaction
ws_prov_utils = DatabricksWSProvisioningUtils(common_params)
# Create object to process AWS stack outputs - to create input data for Workspace Accounts APIs
ws_stack_processor = DatabricksWSStackProcessor()

# Step 1 - Try and deploy the VPC infra cloudformation template
# Read and parse the VPC infra cloudformation template body and required params
vpc_template_data = ws_prov_utils._parse_template('cf_templates/e2-existingvpc-cf_template.json')
vpc_parameter_data = ws_prov_utils._parse_parameters('cf_template_params/e2-existingvpc-cf_params.json')
# Add the extra required params
vpc_parameter_data.append({"ParameterKey": "VpcId","ParameterValue": common_params["vpc_id"]})

# Deploy the VPC infra stack
created_vpc_stack_obj = ws_prov_utils._deploy_stack(common_params["vpc_stack_name"], 
                            vpc_template_data, vpc_parameter_data, False)

# Parse the stack output for security group and subnets that're required to create the workspace network object
network_input_data = ws_stack_processor._process_vpc_stack_output(created_vpc_stack_obj)

# Step 2 - Try and deploy the IAM role cloudformation template
# Read and parse the IAM role cloudformation template body and required params
iam_template_data = ws_prov_utils._parse_template('cf_templates/e2-iam_role_with_restricted_and_sg_policy.json')
iam_parameter_data = ws_prov_utils._parse_parameters('cf_template_params/e2-iam_role_with_restricted_and_sg_policy_params.json')
# Add the extra required params
iam_parameter_data.append({"ParameterKey": "DatabricksAWSAccount","ParameterValue": common_params["databricks_aws_account_id"]})
iam_parameter_data.append({"ParameterKey": "DatabricksE2WorkspaceAccount","ParameterValue": common_params["databricks_workspace_account_id"]})
iam_parameter_data.append({"ParameterKey": "WorkspaceRegion","ParameterValue": common_params["region_name"]})
iam_parameter_data.append({"ParameterKey": "WorkspaceVPC","ParameterValue": common_params["vpc_id"]})
iam_parameter_data.append({"ParameterKey": "WorkspaceSecurityGroup","ParameterValue": network_input_data["security_group_id"]})

# Deploy the IAM role stack
created_iam_stack_obj = ws_prov_utils._deploy_stack(common_params["iam_stack_name"], 
                            iam_template_data, iam_parameter_data, True)

# Parse the stack output for IAM role ARN that's needed to create the workspace credentials object
creds_input_data = ws_stack_processor._process_iam_stack_output(created_iam_stack_obj)

# Step 3 - Create a new S3 root bucket for the Databricks workspace 
# Also add a policy to be accessible from the Databricks control plane
# Read and parse the S3 bucket cloudformation template body and required params
s3_template_data = ws_prov_utils._parse_template('cf_templates/e2-dbfs_root_s3_bucket-cf_template.json')
s3_parameter_data = ws_prov_utils._parse_parameters('cf_template_params/e2-dbfs_root_s3_bucket-cf_params.json')
# Add the extra required params
s3_parameter_data.append({"ParameterKey": "DatabricksAccount","ParameterValue": common_params["databricks_aws_account_id"]})

# Deploy the S3 bucket stack
created_s3_stack_obj = ws_prov_utils._deploy_stack(common_params["s3_stack_name"], 
                            s3_template_data, s3_parameter_data, False)

# Parse the stack info for S3 bucket name that's needed to create the workspace storage config object
storage_config_input_data = ws_stack_processor._process_s3_stack_output(created_s3_stack_obj)

# Step 4 - Try and deploy the KMS key cloudformation template
# Also add a policy to be accessible from the Databricks control plane
# Read and parse the KMS key cloudformation template body and required params
kms_template_data = ws_prov_utils._parse_template('cf_templates/e2-byok_kms_key-cf_template.json')
kms_parameter_data = ws_prov_utils._parse_parameters('cf_template_params/e2-byok_kms_key-cf_params.json')
# Add the extra required params
kms_parameter_data.append({"ParameterKey": "DatabricksAccount","ParameterValue": common_params["databricks_aws_account_id"]})

# Deploy the KMS key stack
created_kms_stack_obj = ws_prov_utils._deploy_stack(common_params["kms_stack_name"], 
                            kms_template_data, kms_parameter_data, False)

# Parse the stack info for KMS Key Alias and ARN that're required to create the workspace customer managed key object
cust_managed_key_input_data = ws_stack_processor._process_kms_stack_output(created_kms_stack_obj)

# Step 5 - Create the workspace credentials object
# First create a Databricks Accounts API Client to use for further work
ws_accounts_api = DatabricksWSAccountsAPI(common_params)

credentials_id = ws_accounts_api._create_credentials(common_params, creds_input_data)

# Step 6 - Create the workspace storage config object
storage_config_id = ws_accounts_api._create_storage_config(common_params, storage_config_input_data)

# Step 7 - Create the workspace network object
network_id = ws_accounts_api._create_network(common_params, network_input_data)

# Step 8 - Create the workspace customer managed key object
customer_managed_key_id = ws_accounts_api._create_customer_managed_key(common_params, cust_managed_key_input_data)

# Step 9 - Create the workspace itself
workspace_input_data = {
    "credentials_id": credentials_id,
    "storage_config_id": storage_config_id,
    "network_id": network_id,
    "customer_managed_key_id": customer_managed_key_id
}
workspace_id = ws_accounts_api._create_workspace(common_params, workspace_input_data)

# Step 10 - Check the workspace provisioning status
workspace_prov_status = ws_accounts_api._check_workspace_provisioning(common_params, {"workspace_id": workspace_id})

print("Final status for the workspace {} is workspace_prov_status is {}".format(workspace_id, workspace_prov_status))
if workspace_prov_status == 'RUNNING':
    deployment_url = "{}://{}.cloud.databricks.com".format("https", common_params["deployment_cname"])
    print("URL for the workspace is {}".format(deployment_url))
