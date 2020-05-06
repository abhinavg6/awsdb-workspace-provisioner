# Interface for specific output processing of AWS stacks created for Databricks E2 Workspaces

class DatabricksWSStackProcessor(object):

    def __init__(self):
        pass

    # Process VPC stack output to get data for workspace network object
    def _process_vpc_stack_output(self, created_stack_obj):
        print("Getting the security group id and subnet ids from VPC stack output")
        vpc_stack_outputs = created_stack_obj['Stacks'][0]['Outputs']
        network_data = {}
        for vpc_stack_output in vpc_stack_outputs:
            if vpc_stack_output['OutputKey'] == 'WorkspaceSecurityGroupOut':
                network_data["security_group_id"] = vpc_stack_output['OutputValue']
                print("Security group id is {}".format(network_data["security_group_id"]))
            elif vpc_stack_output['OutputKey'] == 'Subnet1Out':
                network_data["subnet1_id"] = vpc_stack_output['OutputValue']
                print("Subnet 1 id is {}".format(network_data["subnet1_id"]))
            elif vpc_stack_output['OutputKey'] == 'Subnet2Out':
                network_data["subnet2_id"] = vpc_stack_output['OutputValue']
                print("Subnet 2 id is {}".format(network_data["subnet2_id"]))

        if network_data["security_group_id"] is None or network_data["subnet1_id"] is None or network_data["subnet2_id"] is None:
            print("Exiting the script as security group and/or subnets info was not available in the stack output")
            exit(1)
        return network_data

    # Process IAM stack output to get data for workspace credentials object
    def _process_iam_stack_output(self, created_stack_obj):
        print("Getting the IAM role ARN from IAM stack output")
        iam_stack_outputs = created_stack_obj['Stacks'][0]['Outputs']
        creds_data = {}
        for iam_stack_output in iam_stack_outputs:
            if iam_stack_output['OutputKey'] == 'IAMRoleOut':
                creds_data["iam_role_arn"] = iam_stack_output['OutputValue']
                print("IAM role ARN is {}".format(creds_data["iam_role_arn"]))
                break

        if creds_data["iam_role_arn"] is None:
            print("Exiting the script as IAM role ARN info was not available in the stack output")
            exit(1)
        return creds_data

    # Process S3 stack output to get data for workspace storage config object
    def _process_s3_stack_output(self, created_stack_obj):
        print("Getting the final bucket name from S3 stack output")
        s3_stack_outputs = created_stack_obj['Stacks'][0]['Outputs']
        storage_config_data = {}
        for s3_stack_output in s3_stack_outputs:
            if s3_stack_output['OutputKey'] == 'DBFSRootS3BucketOut':
                storage_config_data["s3_bucket_name_final"] = s3_stack_output['OutputValue']
                print("Final S3 bucket name is {}".format(storage_config_data["s3_bucket_name_final"]))
                break

        if storage_config_data["s3_bucket_name_final"] is None:
            print("Exiting the script as S3 bucket final name was not available in the stack output")
            exit(1)
        return storage_config_data

    # Process KMS stack output to get data for workspace customer managed key object
    def _process_kms_stack_output(self, created_stack_obj):
        print("Getting the KMS Key Alias and ARN from KMS stack output")
        kms_stack_outputs = created_stack_obj['Stacks'][0]['Outputs']
        cust_managed_key_data = {}
        for kms_stack_output in kms_stack_outputs:
            if kms_stack_output['OutputKey'] == 'BYOKKMSKeyOut':
                cust_managed_key_data["kms_key_arn"] = kms_stack_output['OutputValue']
                print("KMS Key ARN is {}".format(cust_managed_key_data["kms_key_arn"]))
            elif kms_stack_output['OutputKey'] == 'BYOKKMSKeyAliasOut':
                # Get the alias after "alias/"
                cust_managed_key_data["kms_key_alias"] = kms_stack_output['OutputValue'][6:]
                print("KMS Key Alias is {}".format(cust_managed_key_data["kms_key_alias"]))
        
        if cust_managed_key_data["kms_key_arn"] is None or cust_managed_key_data["kms_key_alias"] is None:
            print("Exiting the script as KMS Key ARN or Alias info was not available in the stack output")
            exit(1)
        return cust_managed_key_data