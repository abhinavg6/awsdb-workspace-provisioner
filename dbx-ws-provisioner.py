# This is a quick and dirty script to do full end-to-end AWS infra and Databricks E2 workspace
# provisioning. To do something similar in CI/CD pipelines, I would suggest to modularize it
# and externalize config as much config as possible.

import boto3
import botocore
import time

from datetime import datetime
from json import dumps as json_dumps, loads as json_loads

from databricks_cli.sdk import ApiClient
from databricks_cli.accounts import AccountsApi

# Define some helpful functions - could move them to another utils script too
def _json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type not serializable")

def _parse_template(template):
    with open(template) as template_fileobj:
        template_data_in = template_fileobj.read()
    print('Validating {}'.format(template))
    cf_client.validate_template(TemplateBody=template_data_in)
    return template_data_in

def _parse_parameters(parameters):
    with open(parameters) as parameter_fileobj:
        parameter_str = parameter_fileobj.read()
    parameter_data_in = json_loads(parameter_str)
    return parameter_data_in

def _stack_exists(stack_name_in):
    stacks = cf_client.list_stacks()['StackSummaries']
    for stack in stacks:
        if stack['StackStatus'] == 'DELETE_COMPLETE':
            continue
        if stack_name_in == stack['StackName']:
            return True
    return False

# Provide all required master parameters used below
region_name = 'us-west-2'
vpc_id = 'vpc-00000000000000000'
vpc_stack_name = 'E2-BYOVPC-Deploy-AbhiDev'
iam_role_name = 'Databricks-E2-Cross-Account-RestrictedSG-Role-AbhiDev'
iam_stack_name = 'E2-IAMRole-RestrictedSG-Deploy-AbhiDev'
# Get the Databricks AWS account id from your Databricks account team
databricks_aws_acc_id = '111111111111'
# This is the Databricks master account created for your company / business unit
# Check with your admin
databricks_account_id = 'aabbaabb-cccc-dddd-eeee-aaaabbbbaabb'
root_bucket_name = 'globally-unique-bucket-name'
deployment_cname = "e2-abhidev-npip"

# Create boto3 cloudformation client for a given region
session = boto3.Session(profile_name='unique-aws-profile-name')
cf_client = session.client(service_name='cloudformation', region_name=region_name)

# Read, parse and build the VPC infra cloudformation template body and required params
vpc_template_data = _parse_template('cf_templates/e2-existingvpc-cf_template.json')
vpc_parameter_data = _parse_parameters('cf_template_params/e2-existingvpc-cf_params.json')
vpc_parameter_data.append({"ParameterKey": "VpcId","ParameterValue": vpc_id})

# Step1 - Try and deploy the VPC infra cloudformation template
vpc_stack_deploy_try = False
created_vpc_stack_obj = None
try:
    if not _stack_exists(vpc_stack_name):
        vpc_stack_deploy_try = True
        print('Creating stack {}'.format(vpc_stack_name))
        vpc_stack_result = cf_client.create_stack(StackName=vpc_stack_name, 
                                    TemplateBody=vpc_template_data, Parameters=vpc_parameter_data)
        vpc_stack_waiter = cf_client.get_waiter('stack_create_complete')
        print("...Waiting for stack {} to be created...".format(vpc_stack_name))
        vpc_stack_waiter.wait(StackName=vpc_stack_name, WaiterConfig={'Delay': 10,'MaxAttempts': 90})
except botocore.exceptions.ClientError as ex:
    error_message = ex.response['Error']['Message']
    print(error_message)
    raise
else:
    if vpc_stack_deploy_try:
        created_vpc_stack_obj = cf_client.describe_stacks(StackName=vpc_stack_result['StackId'])
        print(json_dumps(created_vpc_stack_obj, indent=2, default=_json_serial))

if created_vpc_stack_obj is None or created_vpc_stack_obj['Stacks'][0]['StackStatus'] != 'CREATE_COMPLETE':
    print("Exiting the script as VPC stack either already exists or it was not created successfully")
    exit(1)

# Parse the stack info for security group and subnets that're required later
print("Getting the security group id from VPC stack output")
vpc_stack_outputs = created_vpc_stack_obj['Stacks'][0]['Outputs']
subnet1_id = None
subnet2_id = None
security_group_id = None
for vpc_stack_output in vpc_stack_outputs:
    if vpc_stack_output['OutputKey'] == 'WorkspaceSecurityGroupOut':
        security_group_id = vpc_stack_output['OutputValue']
        print("Security group id is {}".format(security_group_id))
    elif vpc_stack_output['OutputKey'] == 'Subnet1Out':
        subnet1_id = vpc_stack_output['OutputValue']
        print("Subnet 1 id is {}".format(subnet1_id))
    elif vpc_stack_output['OutputKey'] == 'Subnet2Out':
        subnet2_id = vpc_stack_output['OutputValue']
        print("Subnet 2 id is {}".format(subnet2_id))

if security_group_id is None or subnet1_id is None or subnet2_id is None:
    print("Exiting the script as security group and/or subnets info was not available in the stack output")
    exit(1)

# Read, parse and build the IAM role cloudformation template body and required params
iam_template_data = _parse_template('cf_templates/e2-iam_role_with_restricted_and_sg_policy.json')
iam_parameter_data = _parse_parameters('cf_template_params/e2-iam_role_with_restricted_and_sg_policy_params.json')
# Add the extra required params
iam_parameter_data.append({"ParameterKey": "DatabricksAccount","ParameterValue": databricks_aws_acc_id})
iam_parameter_data.append({"ParameterKey": "IAMRoleName","ParameterValue": iam_role_name})
iam_parameter_data.append({"ParameterKey": "WorkspaceRegion","ParameterValue": region_name})
iam_parameter_data.append({"ParameterKey": "WorkspaceVPC","ParameterValue": vpc_id})
iam_parameter_data.append({"ParameterKey": "WorkspaceSecurityGroup","ParameterValue": security_group_id})

# Step 2 - Try and deploy the IAM role cloudformation template
iam_stack_deploy_try = False
created_iam_stack_obj = None
try:
    if not _stack_exists(iam_stack_name):
        iam_stack_deploy_try = True
        print('Creating stack {}'.format(iam_stack_name))
        iam_stack_result = cf_client.create_stack(StackName=iam_stack_name, 
                                    TemplateBody=iam_template_data, Parameters=iam_parameter_data,
                                    Capabilities=['CAPABILITY_NAMED_IAM'])
        iam_stack_waiter = cf_client.get_waiter('stack_create_complete')
        print("...Waiting for stack {} to be created...".format(iam_stack_name))
        iam_stack_waiter.wait(StackName=iam_stack_name, WaiterConfig={'Delay': 10,'MaxAttempts': 60})
except botocore.exceptions.ClientError as ex:
    error_message = ex.response['Error']['Message']
    print(error_message)
    raise
else:
    if iam_stack_deploy_try:
        created_iam_stack_obj = cf_client.describe_stacks(StackName=iam_stack_result['StackId'])
        print(json_dumps(created_iam_stack_obj, indent=2, default=_json_serial))

if created_iam_stack_obj is None or created_iam_stack_obj['Stacks'][0]['StackStatus'] != 'CREATE_COMPLETE':
    print("Exiting the script as IAM stack either already exists or it was not created successfully")
    exit(1)

# Parse the stack info for IAM role ARN that's needed to create the workspace credentials
print("Getting the IAM role ARN from IAM stack output")
iam_stack_outputs = created_iam_stack_obj['Stacks'][0]['Outputs']
iam_role_arn = None
for iam_stack_output in iam_stack_outputs:
    if iam_stack_output['OutputKey'] == 'IAMRoleOut':
        iam_role_arn = iam_stack_output['OutputValue']
        print("IAM role ARN is {}".format(iam_role_arn))
        break

if iam_role_arn is None:
    print("Exiting the script as IAM role ARN info was not available in the stack output")
    exit(1)

# Create a Databricks CLI API Client to use for further work
# This is quick and dirty, better read required params from externalized config
# Get the credentials from your admin
dbcli_apiclient = ApiClient(user='abhinav.garg@databricks.com', password='password',
                            host='https://whatever.cloud.databricks.com', 
                            verify=True, command_name='Python Dev')
accounts_api_client = AccountsApi(dbcli_apiclient)

# Step 3 - Create the workspace credentials object
print("Creating the Databricks workspace credentials")
credentials_id = None
credentials_request = {
  "credentials_name": "e2-gtm-test-workspace-abhidev-creds-2",
  "aws_credentials": {
    "sts_role": {
      "role_arn": iam_role_arn
    }
  }
}
credentials_resp = accounts_api_client.create_credentials(databricks_account_id, credentials_request)
credentials_id = credentials_resp['credentials_id']
external_id = credentials_resp['aws_credentials']['sts_role']['external_id']
print("Credentials id is {} and External id is {}".format(credentials_id, external_id))

if credentials_id is None or external_id is None:
    print("Exiting the script as credentials object was not created successfully")
    exit(1)

# Step 4 - Create IAM role resource and update its trust policy with the external id
print("Creating the IAM role resource")
iam_resource = session.resource('iam')
iam_assume_role_policy = iam_resource.AssumeRolePolicy(iam_role_name)

databricks_aws_principal = "arn:aws:iam::{}:root".format(databricks_aws_acc_id)
assume_role_policy_doc = {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": databricks_aws_principal
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": external_id
        }
      }
    }
  ]
}

print("Trying to update the IAM assume role policy with external id")
iam_assume_role_policy.update(PolicyDocument=json_dumps(assume_role_policy_doc))
print("IAM assume role policy updated successfully")

# Step 5 - Create a new S3 root bucket for the Databricks workspace 
# Also add a policy to be accessible from the Databricks control plane
print("Creating the S3 bucket resource")
s3_resource = session.resource(service_name='s3', region_name=region_name)

is_bucket_created = False
try:
    print("Creating the S3 bucket {}".format(root_bucket_name))
    bucket = s3_resource.Bucket(root_bucket_name)
    bucket_created_resp = bucket.create(ACL='private',CreateBucketConfiguration={'LocationConstraint':region_name})
    print(json_dumps(bucket_created_resp))

    print("Updating the bucket policy")
    bucket_policy = bucket.Policy()
    bucket_resource_path1 = "arn:aws:s3:::{}/*".format(root_bucket_name)
    bucket_resource_path2 = "arn:aws:s3:::{}".format(root_bucket_name)
    bucket_policy_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Grant Databricks Access",
                "Effect": "Allow",
                "Principal": {
                    "AWS": databricks_aws_principal
                },
                "Action": [
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Resource": [
                    bucket_resource_path1,
                    bucket_resource_path2
                ]
            }
        ]
    }

    bucket_policy_updated_resp = bucket_policy.put(ConfirmRemoveSelfBucketAccess=False, 
                                                Policy=json_dumps(bucket_policy_doc))
    print(json_dumps(bucket_policy_updated_resp))
    is_bucket_created = True
except botocore.exceptions.ClientError as ex:
    error_message = ex.response['Error']['Message']
    print(error_message)
    raise

if not is_bucket_created:
    print("Exiting the script as root S3 bucket was not created successfully")
    exit(1)

# Step 6 - Create the workspace storage config object
print("Creating the Databricks workspace storage config")
storage_config_id = None
storage_config_request = {
	"storage_configuration_name": "e2-gtm-test-workspace-abhidev-storage-config",
	"root_bucket_info": {
		"bucket_name": root_bucket_name
	}
}
storage_config_resp = accounts_api_client.create_storage_config(databricks_account_id, storage_config_request)
storage_config_id = storage_config_resp['storage_configuration_id']
print("Storage config id is {}".format(storage_config_id))

if storage_config_id is None:
    print("Exiting the script as storage config object was not created successfully")
    exit(1)

# Step 7 - Create the workspace network object
print("Creating the Databricks workspace network")
network_id = None
network_request = {
	"network_name": "e2-gtm-test-workspace-abhidev-net",
	"vpc_id": vpc_id,
	"subnet_ids": [subnet1_id, subnet2_id],
	"security_group_ids": [security_group_id]
}
network_resp = accounts_api_client.create_network(databricks_account_id, network_request)
network_id = network_resp['network_id']
print("Network id is {}".format(network_id))

if network_id is None:
    print("Exiting the script as network object was not created successfully")
    exit(1)

# Step 8 - Create the workspace itself
print("Creating the Databricks workspace network")
workspace_id = None
workspace_request = {
	"workspace_name": "e2-gtm-test-abhidev-workspace",
	"deployment_name": deployment_cname,
	"aws_region": region_name,
	"credentials_id": credentials_id,
	"network_id": network_id,
	"storage_configuration_id": storage_config_id,
	"is_no_public_ip_enabled": True
}
workspace_resp = accounts_api_client.create_workspace(databricks_account_id, workspace_request)
workspace_id = workspace_resp['workspace_id']
print("Workspace id is {}".format(workspace_id))

if workspace_id is None:
    print("Exiting the script as workspace request was not posted successfully")
    exit(1)

workspace_prov_status = 'PROVISIONING'
while workspace_prov_status == 'PROVISIONING':
    time.sleep(5)
    workspace_prov_resp = accounts_api_client.get_workspace(databricks_account_id, workspace_id)
    workspace_prov_status = workspace_prov_resp['workspace_status']

print("Final status for the workspace {} is workspace_prov_status is {}".format(workspace_id, workspace_prov_status))
if workspace_prov_status == 'RUNNING':
    deployment_url = "{}://{}.cloud.databricks.com".format("https", deployment_cname)
    print("URL for the workspace is {}".format(deployment_url))
