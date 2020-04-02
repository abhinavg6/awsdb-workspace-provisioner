# awsdb-workspace-provisioner
Sample Provisioning Project for AWS Databricks E2 Workspace

## Project Structure
* dbx-ws-provisioner.py: A single monolith script to provision a Databricks AWS E2 workspace and its required AWS infrastructure end-to-end in single-shot. Please feel free to clone and modularize for your Databricks provisioning CI/CD pipelines.
* cf_templates: Contains two sample templates that are used by the provisioning script - one to provision the necessary networking infra in an existing VPC, and another to create a restricted IAM role required by Databricks.
* cf_template_params: Base parameters for the above CF templates. Some dynamic parameters are added within the script.

## Flow of the Script
* Create the necessary networking infra in an existing VPC, using Cloudformation
* Create the cross-account IAM role required by Databricks, using Cloudformation (it uses some of the output values from first step)
* Create the Databricks workspace credentials (using the above IAM role ARN)
* Update the cross-account IAM role's trust policy with an external id from Databricks credentials
* Create a root S3 bucket for the Databricks workspace with a bucket policy allowing relevant access by Databricks
* Create the Databricks workspace storage configuration (using the above S3 bucket name)
* Create the Databricks workspace network (using the references to above networking infra)
* Finally create the Databricks workspace (using the references to above credentials, storage configuration and network). It waits until the workspace has been provisioned.

## Running the Project
* Clone the repo
* `pip install boto3` or `conda install boto3` - This should get `botocore` as well if not there already.
* `pip install git+git://github.com/abhinavg6/databricks-cli.git` - This is a fork synced from main Databricks CLI, and contains the preview E2 account API.
* Provide relevant param values for cloudformation templaes as per your environment. See [this template repo](https://github.com/abhinavg6/awsdb-cf-templates-ext) for updated templates.
* Provide relevant master parameter values in the python script as per your environment.
* If you're changing the template structure or using a different template altogether, just make sure that relevant parameters and output values are referenced in the python script.
