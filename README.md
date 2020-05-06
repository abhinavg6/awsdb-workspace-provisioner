# awsdb-workspace-provisioner
Sample Provisioning Project for AWS Databricks E2 Workspace

## Project Structure
* dbx_ws_provisioner.py: Controller script to provision a Databricks AWS E2 workspace and its required AWS infrastructure end-to-end in single pass.
* dbx_ws_utils.py: Utility interface with primary purpose of interacting with AWS Cloudformation in order to deploy stacks.
* dbx_ws_stack_processor.py: Processor interface with primary purpose of processing AWS stack output to get input data for Workspace Accounts APIs.
* dbx_ws_accounts_api.py: API interface with primary purpose of creating required objects for a Databricks E2 Workspace.
* commons_params.json: A set of common parameters that should be used across the infrastructure components and workspace objects.
* cf_templates: Contains cloudformation templates that are used by the provisioning script - to create the necessary networking infra in an existing VPC with existing NAT gateway, to create a restricted IAM role required by Databricks, to create a DBFS root S3 bucket for the workspace, and to create a BYOK KMS key for the workspace notebooks.
* cf_template_params: Base parameters for the above cloudformation templates.

## Flow of the Script
* Create the necessary networking infra in an existing VPC, using Cloudformation
* Create the cross-account IAM role required by Databricks, using Cloudformation (it uses some of the output values from first step)
* Create the DBFS root S3 bucket for the Databricks workspace, using Cloudformation
* Create the BYOK KMS key for the Databricks workspace notebooks, using Cloudformation
* Create the Databricks workspace credentials object (using the above IAM role ARN)
* Create the Databricks workspace storage config object (using the above S3 bucket name)
* Create the Databricks workspace network object (using the references to above networking infra)
* Create the Databricks workspace customer managed key object (using the above KMS key ARN and Alias)
* Finally create the Databricks workspace (using the references to above credentials, storage configuration, network and customer managed key objects). It waits until the workspace has been provisioned.

## Running the Project
* Clone the repo
* `pip install boto3` or `conda install boto3` - This should get `botocore` as well if not there already.
* `pip install git+git://github.com/abhinavg6/databricks-cli.git` - This is a fork synced from main Databricks CLI, and contains the preview E2 account API.
* Make sure that the relevant [AWS user credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#shared-credentials-file) exist in the home directory at `~/.aws/credentials`. 
* Provide relevant param values for cloudformation templates in *params.json files as per your environment. See [this template repo](https://github.com/abhinavg6/awsdb-cf-templates-ext) for updated templates.
* Provide relevant master parameter values in common_params.json as per your environment.
* If you're changing the template structure or using a different template altogether, just make sure that relevant parameters and output values are referenced in the scripts.
* Execute as `python dbx_ws_provisioner.py`

**Note:** Databricks E2 on AWS is currently a private preview functionality that requires Databricks to create a master account id and whitelist relevant operations in order to create E2 workspaces. Please reach out to your Databricks account team before starting to use this sample solution.
