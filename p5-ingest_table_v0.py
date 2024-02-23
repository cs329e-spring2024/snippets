"""A first dag that ingests a raw table into BQ."""
import airflow
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import timedelta

project_id = 'cs329e-sp2024'
dataset_name = 'airline_raw_auto'
table_name = 'air_carriers'
bucket_name = 'cs329e-open-access'
file_name = 'initial_load/air_carriers.csv'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

air_carriers_schema_full = [
    {"name": "code", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "description", "type": "STRING", "mode": "REQUIRED"},
    {"name": "load", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

air_carriers_schema = [
    {"name": "code", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "description", "type": "STRING", "mode": "REQUIRED"},
]

dag = DAG(
    'p5-ingest_table_v0',
    default_args=default_args,
    schedule_interval='@once',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

# create dataset
t1 = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_if_needed',
    project_id=project_id,
    dataset_id=dataset_name,
    location='us-central1',
    if_exists='ignore',
    dag=dag)

# create air_carriers table
t2 = BigQueryCreateEmptyTableOperator(
    task_id='create_air_carriers',
    project_id=project_id,
    dataset_id=dataset_name,
    table_id=table_name,
    schema_fields=air_carriers_schema_full,
    if_exists='ignore',
    dag=dag)

t3 = GCSToBigQueryOperator(
    task_id='load_air_carriers',
    bucket=bucket_name,
    source_objects=[file_name],
    destination_project_dataset_table=f"{dataset_name}.{table_name}",
    schema_fields=air_carriers_schema,
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

# dependencies
t1 >> t2 >> t3
