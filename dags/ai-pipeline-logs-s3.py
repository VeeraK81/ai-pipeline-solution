import os
import logging
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.configuration import conf
from datetime import datetime, timezone
from datetime import datetime
from airflow.decorators import task
from airflow.models.dag import DAG
import os
from io import StringIO

# DAG Configuration
DAG_ID = 'ai_logs'
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
    description="Save airflow logs to Data Lake",
    catchup=False,
    tags=['airflow', 's3'],
) as dag:
    
    @task
    def write_logs_s3():
        # S3 Configuration
        s3_hook = S3Hook(aws_conn_id="aws_default")
        S3_BUCKET_NAME = "flow-bucket-ml"
        S3_KEY_PREFIX = "logs/airflow_air_quality_logs"

        # Base log folder from Airflow configuration
        base_log_folder = conf.get('logging', 'base_log_folder')
        
        # Current timestamp for unique file naming
        timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        consolidated_log_file = f"airflow_logs_{timestamp}.txt"

        try:
            with open(consolidated_log_file, 'w') as log_file:
                logging.info(f"Collecting logs from {base_log_folder}...")

                # Walk through the base log folder
                for root, dirs, files in os.walk(base_log_folder):
                    for file in files:
                        log_path = os.path.join(root, file)

                        # Append each log file content to the consolidated file
                        try:
                            with open(log_path, 'r') as f:
                                log_file.write(f"--- Log file: {log_path} ---\n")
                                log_file.write(f.read())
                                log_file.write("\n\n")
                        except Exception as e:
                            logging.warning(f"Could not read log file {log_path}: {str(e)}")

            # Upload the consolidated log file to S3
            s3_key = f"{S3_KEY_PREFIX}/{consolidated_log_file}"
            logging.info(f"Uploading consolidated log file to S3: {S3_BUCKET_NAME}/{s3_key}")
            s3_hook.load_file(
                filename=consolidated_log_file,
                key=s3_key,
                bucket_name=S3_BUCKET_NAME,
                replace=True
            )

            # Cleanup: Remove the local consolidated log file
            os.remove(consolidated_log_file)
            logging.info("Logs uploaded to S3 and local file cleaned up.")

        except Exception as e:
            logging.error(f"Error during log collection or S3 upload: {str(e)}")
            raise

    
    #     # Task Chaining (DAG Workflow)
    write_logs_task = write_logs_s3()
    
    write_logs_task