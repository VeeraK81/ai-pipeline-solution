
from airflow.decorators import task
from airflow.configuration import conf
from airflow.models.dag import DAG
import os
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from io import StringIO, BytesIO

# DAG Configuration
DAG_ID = 'ai_logs'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id=DAG_ID,
    schedule_interval='0 1 * * *',
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

        # Get today's date
        today = datetime.utcnow().date()

        # Current timestamp for unique file naming
        timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        consolidated_log_file = f"airflow_logs_{timestamp}.txt"

        try:
            # Use StringIO for in-memory log consolidation (avoid writing to disk)
            log_content = StringIO()
            logging.info(f"Collecting today's logs from {base_log_folder}...")

            # Walk through the base log folder and filter today's logs based on file modification date
            for root, dirs, files in os.walk(base_log_folder):
                for file in files:
                    log_path = os.path.join(root, file)

                    # Check if the log file was modified today
                    try:
                        file_modified_time = datetime.utcfromtimestamp(os.path.getmtime(log_path)).date()
                        if file_modified_time == today:
                            log_content.write(f"--- Log file: {log_path} ---\n")
                            with open(log_path, 'r') as f:
                                log_content.write(f.read())
                                log_content.write("\n\n")
                    except Exception as e:
                        logging.warning(f"Could not read or process log file {log_path}: {str(e)}")

            # If there are logs collected for today
            if log_content.tell() > 0:
                # Reset the StringIO object to the beginning before reading
                log_content.seek(0)

                # Convert the StringIO content to bytes
                log_bytes = log_content.getvalue().encode('utf-8')

                # Wrap the bytes in a BytesIO object to make it file-like
                log_file_obj = BytesIO(log_bytes)

                # Upload the consolidated log content to S3 directly
                s3_key = f"{S3_KEY_PREFIX}/{consolidated_log_file}"
                logging.info(f"Uploading consolidated log file to S3: {S3_BUCKET_NAME}/{s3_key}")
                s3_hook.load_file_obj(
                    file_obj=log_file_obj,
                    key=s3_key,
                    bucket_name=S3_BUCKET_NAME,
                    replace=True
                )

                logging.info("Today's logs uploaded to S3.")
            else:
                logging.info("No logs found for today.")

        except Exception as e:
            logging.error(f"Error during log collection or S3 upload: {str(e)}")
            raise

    # Task Chaining (DAG Workflow)
    write_logs_task = write_logs_s3()
    
    write_logs_task