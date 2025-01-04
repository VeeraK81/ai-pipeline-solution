import boto3
import time
from datetime import datetime
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import os
from pymongo import MongoClient



# Get AWS connection details from Airflow
KEY_PAIR_NAME=Variable.get("KEY_PAIR_NAME")
KEY_PATH = Variable.get("KEY_PATH")  # Path to your private key inside the container
SECURITY_GROUP_ID=Variable.get("SECURITY_GROUP_ID")
aws_conn = BaseHook.get_connection('aws_default')  # Use the Airflow AWS connection
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password
region_name = aws_conn.extra_dejson.get('region_name', 'eu-west-3')  # Default to 'eu-west-3'

AWS_ACCESS_KEY_ID= aws_access_key_id
AWS_SECRET_ACCESS_KEY=aws_secret_access_key
BUCKET_NAME = Variable.get("BUCKET_NAME")
FILE_KEY = Variable.get("FILE_KEY")
ARTIFACT_ROOT = Variable.get("ARTIFACT_ROOT")


# DAG Configuration
DAG_ID = 'ai_data_ingestion_dag'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id=DAG_ID,
    schedule_interval='@daily',
    default_args=default_args,
    description="Data ingestion from NoSQL to Data Lake",
    catchup=False,
    tags=['mongo', 's3'],
) as dag:

    # Step 1: Poll Jenkins Job Status
    @task
    def query_NoSQL():
                
        try:
            mongo_uri = os.getenv('MONGO_URI')
            client = MongoClient(mongo_uri)
            db = client.get_database('mydatabase')
            collection = db.get_collection('airflow_logs')

            logs = collection.find({"task_id": "test_logging_task01"})
            for log in logs:
                print("Connected mongo success ------------------------------------------:", log)
        except Exception as e:
            print(f"Error occurred during mongo connection: {str(e)}")
            raise
        finally:
            # Close the SSH connection
            client.close()


#     # Task Chaining (DAG Workflow)
    test = query_NoSQL()
    
    test
