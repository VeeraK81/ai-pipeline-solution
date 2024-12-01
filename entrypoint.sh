#!/bin/bash

# Read the contents of the DBURL secret file into the environment variable
if [ -f /tmp/AI_DB_URL ]; then
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$(cat /tmp/AI_DB_URL)
else
  echo "Error: /tmp/AI_DB_URL not found."
  exit 1
fi


airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin


rm /tmp/AI_DB_URL 

# Execute the passed command (this will run the Airflow command or any other passed command)
exec "$@"
