import boto3
import time
from datetime import datetime
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2TerminateInstanceOperator,
)
from airflow.hooks.base import BaseHook
import paramiko
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import os

# Jenkins Configuration: Load from Airflow Variables
JENKINS_URL = Variable.get("JENKINS_URL")
JENKINS_USER = Variable.get("JENKINS_USER")
JENKINS_TOKEN = Variable.get("JENKINS_TOKEN")
JENKINS_JOB_NAME = Variable.get("JENKINS_JOB_NAME")

# Get AWS connection details from Airflow
KEY_PAIR_NAME=Variable.get("KEY_PAIR_NAME")
KEY_PATH = Variable.get("KEY_PATH")  # Path to your private key inside the container
AMI_ID=Variable.get("AMI_ID")
SECURITY_GROUP_ID=Variable.get("SECURITY_GROUP_ID")
INSTANCE_TYPE=Variable.get("INSTANCE_TYPE")
aws_conn = BaseHook.get_connection('aws_default')  # Use the Airflow AWS connection
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password
region_name = aws_conn.extra_dejson.get('region_name', 'eu-west-3')  # Default to 'eu-west-3'

# Retrieve other env variables for MLFlow to run
MLFLOW_TRACKING_URI=Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_ID=Variable.get("MLFLOW_EXPERIMENT_ID")
AWS_ACCESS_KEY_ID= aws_access_key_id
AWS_SECRET_ACCESS_KEY=aws_secret_access_key
BUCKET_NAME = Variable.get("BUCKET_NAME")
FILE_KEY = Variable.get("FILE_KEY")
ARTIFACT_ROOT = Variable.get("ARTIFACT_ROOT")


if not all([JENKINS_URL, JENKINS_USER, JENKINS_TOKEN]):
    raise ValueError("Missing one or more Jenkins configuration environment variables")

# DAG Configuration
DAG_ID = 'ai_jenkins_ec2_training_dag'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id=DAG_ID,
    schedule_interval='0 1 * * *',
    default_args=default_args,
    description="Poll Jenkins, launch EC2, and run ML training",
    catchup=False,
    tags=['jenkins', 'ec2', 'ml-training'],
) as dag:


    
    # Step 1: Create EC2 Instance Using EC2 Operator
    create_ec2_instance = EC2CreateInstanceOperator(
        task_id="create_ec2_instance",
        image_id= AMI_ID,  
        max_count=1,
        min_count=1,
        config={  # Dictionary for arbitrary parameters to the boto3 `run_instances` call
            "InstanceType": INSTANCE_TYPE,
            "KeyName": KEY_PAIR_NAME,  
            "SecurityGroupIds": [SECURITY_GROUP_ID],  
            "TagSpecifications": [
                {
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Purpose', 'Value': 'ML-Training'}]
                }
            ]
        },
        wait_for_completion=True,  # Wait for the instance to be running before proceeding
    )

    # Step 2: Use EC2 Sensor to Check if Instance is Running
    @task
    def check_ec2_status(instance_id):
        """Check if the EC2 instance has passed both status checks (2/2 checks passed)."""
        
        ec2_client = boto3.client(
            'ec2', 
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )  
        passed_checks = False
        
        while not passed_checks:
            # Get the instance status
            response = ec2_client.describe_instance_status(InstanceIds=instance_id)

            # Check if there is any status information returned
            if response['InstanceStatuses']:
                instance_status = response['InstanceStatuses'][0]

                system_status = instance_status['SystemStatus']['Status']
                instance_status_check = instance_status['InstanceStatus']['Status']
                
                # Log the current status
                print(f"System Status: {system_status}, Instance Status: {instance_status_check}")
                
                # Check if both status checks are passed
                if system_status == 'ok' and instance_status_check == 'ok':
                    print(f"Instance {instance_id} has passed 2/2 status checks.")
                    passed_checks = True
                else:
                    print(f"Waiting for instance {instance_id} to pass 2/2 status checks...")
            else:
                print(f"No status available for instance {instance_id} yet. Waiting...")

            # Wait before polling again
            time.sleep(15)

        return True

    # Step 3: Define Run Training as an @task to Get EC2 Public IP
    @task
    def get_ec2_public_ip(instance_id):
        """Retrieve the EC2 instance public IP for SSH."""
    

        # Initialize the EC2 resource using boto3 with credentials from Airflow connection
        ec2 = boto3.resource(
            'ec2',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        # Access EC2 instance by ID
        instance = ec2.Instance(instance_id[0])

        # Wait for the instance to be running
        instance.wait_until_running()
        instance.reload()

        # Get the instance's public IP
        public_ip = instance.public_ip_address
        print(f"Public IP of EC2 Instance: {public_ip}")

        # Return the public IP for the SSH task
        return public_ip


    # Step 4: Terminate EC2 Instance
    terminate_instance = EC2TerminateInstanceOperator(
        task_id="terminate_ec2_instance",
        instance_ids="{{ task_instance.xcom_pull(task_ids='create_ec2_instance', key='return_value')[0] }}",
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )


#     # Task Chaining (DAG Workflow)
    ec2_public_ip = get_ec2_public_ip(create_ec2_instance.output)
    check_ec2_instance=check_ec2_status(create_ec2_instance.output)
    
    # Define task dependencies
    create_ec2_instance >> check_ec2_instance >> ec2_public_ip >> terminate_instance
