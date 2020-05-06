# Interface for utility methods aiding in AWS Databricks E2 Workspace provisioning
# Majorly a client for AWS cloudformation interaction, and any pre or post processing

import boto3
import botocore

from datetime import datetime
from json import dumps as json_dumps, loads as json_loads

class DatabricksWSProvisioningUtils(object):

    def __init__(self, common_params):
        session = boto3.Session(profile_name='databricks-field-eng-admin')
        self.cf_client = session.client(service_name='cloudformation', region_name=common_params["region_name"])

    # Appropriately serialize the JSON to be printed/dumped correctly
    def _json_serial(self, obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, datetime):
            serial = obj.isoformat()
            return serial
        raise TypeError("Type not serializable")

    # Read, parse and validate a AWS cloudformation templae
    def _parse_template(self, template):
        with open(template) as template_fileobj:
            template_data_in = template_fileobj.read()
        print('Validating {}'.format(template))
        self.cf_client.validate_template(TemplateBody=template_data_in)
        return template_data_in

    # Read and parse a parameter file, including both app-specific and AWS cloudformation parameters
    def _parse_parameters(self, parameters):
        with open(parameters) as parameter_fileobj:
            parameter_str = parameter_fileobj.read()
        parameter_data_in = json_loads(parameter_str)
        return parameter_data_in

    # Check if a AWS cloudformation stack exists or not
    def _stack_exists(self, stack_name_in):
        stacks = self.cf_client.list_stacks()['StackSummaries']
        for stack in stacks:
            if stack['StackStatus'] == 'DELETE_COMPLETE':
                continue
            if stack_name_in == stack['StackName']:
                return True
        return False

    # Deploy a AWS cloudformation stack
    def _deploy_stack(self, stack_name, template_data, parameter_data, is_iam_stack):
        stack_deploy_try = False
        created_stack_obj = None
        try:
            if not self._stack_exists(stack_name):
                stack_deploy_try = True
                print('Creating stack {}'.format(stack_name))
                if(is_iam_stack):
                    stack_result = self.cf_client.create_stack(StackName=stack_name, 
                                        TemplateBody=template_data, Parameters=parameter_data,
                                        Capabilities=['CAPABILITY_NAMED_IAM'])
                else:
                    stack_result = self.cf_client.create_stack(StackName=stack_name, 
                                        TemplateBody=template_data, Parameters=parameter_data)
                stack_waiter = self.cf_client.get_waiter('stack_create_complete')
                print("...Waiting for stack {} to be created...".format(stack_name))
                stack_waiter.wait(StackName=stack_name, WaiterConfig={'Delay': 10,'MaxAttempts': 90})
        except botocore.exceptions.ClientError as ex:
            error_message = ex.response['Error']['Message']
            print(error_message)
            raise
        else:
            if stack_deploy_try:
                created_stack_obj = self.cf_client.describe_stacks(StackName=stack_result['StackId'])
                print(json_dumps(created_stack_obj, indent=2, default=self._json_serial))

        if created_stack_obj is None or created_stack_obj['Stacks'][0]['StackStatus'] != 'CREATE_COMPLETE':
            print("Exiting the script as stack {} either already exists or it was not created successfully".format(stack_name))
            exit(1)
        return created_stack_obj