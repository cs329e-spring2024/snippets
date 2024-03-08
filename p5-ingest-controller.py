import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

project_id = 'cs329e-sp2024'
dataset_name = 'airline_raw_af'
bucket_name = 'cs329e-open-access'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='p5-ingest-controller',
    default_args=default_args,
    description='controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

ingest_start = DummyOperator(task_id="ingest_start", dag=dag)
ingest_end = DummyOperator(task_id="ingest_end", dag=dag)

# create dataset raw
create_dataset_raw = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_raw',
    project_id=project_id,
    dataset_id=dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# load air_carriers
air_carriers_table_name = 'air_carriers'
air_carriers_file_name = 'initial_load/air_carriers.csv'
air_carriers_delimiter = ','
air_carriers_skip_leading_rows = 1

air_carriers_schema_full = [
    {"name": "code", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "description", "type": "STRING", "mode": "REQUIRED"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

air_carriers_schema = [
    {"name": "code", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "description", "type": "STRING", "mode": "REQUIRED"},
]

air_carriers_raw = TriggerDagRunOperator(
    task_id="air_carriers_raw",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": air_carriers_table_name, "file_name": air_carriers_file_name, \
           "delimiter": air_carriers_delimiter, "skip_leading_rows": air_carriers_skip_leading_rows, \
           "schema_full": air_carriers_schema_full, "schema": air_carriers_schema},
    dag=dag)


# load bird_airports
bird_airports_table_name = 'bird_airports'
bird_airports_file_name = 'initial_load/bird_airports.csv'
bird_airports_delimiter = ','
bird_airports_skip_leading_rows = 1

bird_airports_schema_full = [
  {"name": "code", "type": "STRING", "mode": "NULLABLE"},
  {"name": "description", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

bird_airports_schema = [
  {"name": "code", "type": "STRING", "mode": "NULLABLE"},
  {"name": "description", "type": "STRING", "mode": "NULLABLE"},
]

bird_airports_raw = TriggerDagRunOperator(
    task_id="bird_airports_raw",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": bird_airports_table_name, "file_name": bird_airports_file_name, \
           "delimiter": bird_airports_delimiter, "skip_leading_rows": bird_airports_skip_leading_rows, \
           "schema_full": bird_airports_schema_full, "schema": bird_airports_schema},
    dag=dag)
    

# load faker_airports
faker_airports_table_name = 'faker_airports'
faker_airports_file_name = 'initial_load/faker_airports.csv'
faker_airports_delimiter = "|"
faker_airports_skip_leading_rows = 1

faker_airports_schema_full = [
  {"name": "airport", "type": "STRING", "mode": "REQUIRED"},
  {"name": "iata", "type": "STRING", "mode": "REQUIRED"},
  {"name": "icao", "type": "STRING", "mode": "NULLABLE"},
  {"name": "city", "type": "STRING", "mode": "NULLABLE"},
  {"name": "state", "type": "STRING", "mode": "NULLABLE"},
  {"name": "country", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

faker_airports_schema = [
  {"name": "airport", "type": "STRING", "mode": "REQUIRED"},
  {"name": "iata", "type": "STRING", "mode": "REQUIRED"},
  {"name": "icao", "type": "STRING", "mode": "NULLABLE"},
  {"name": "city", "type": "STRING", "mode": "NULLABLE"},
  {"name": "state", "type": "STRING", "mode": "NULLABLE"},
  {"name": "country", "type": "STRING", "mode": "NULLABLE"},
]

faker_airports_raw = TriggerDagRunOperator(
    task_id="faker_airports_raw",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": faker_airports_table_name, "file_name": faker_airports_file_name, \
           "delimiter": faker_airports_delimiter, "skip_leading_rows": faker_airports_skip_leading_rows, \
           "schema_full": faker_airports_schema_full, "schema": faker_airports_schema},
    dag=dag)


# load airlines
airlines_table_name = 'airlines'
airlines_file_name = 'initial_load/airlines.csv'
airlines_delimiter = ","
airlines_skip_leading_rows = 1

airlines_schema_full = [
  {"name": "fl_date", "type": "STRING", "mode": "REQUIRED"},
  {"name": "op_carrier_airline_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "tail_num", "type": "STRING", "mode": "NULLABLE"},
  {"name": "op_carrier_fl_num", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "origin_airport_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "origin_airport_seq_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "origin_city_market_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "origin", "type": "STRING", "mode": "NULLABLE"},
  {"name": "dest_airport_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dest_airport_seq_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dest_city_market_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dest", "type": "STRING", "mode": "NULLABLE"},
  {"name": "crs_dep_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dep_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dep_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dep_delay_new", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "arr_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "arr_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "arr_delay_new", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "cancelled", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "cancellation_code", "type": "STRING", "mode": "NULLABLE"},
  {"name": "crs_elapsed_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "actual_elapsed_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "carrier_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "weather_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "nas_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "security_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "late_aircraft_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

airlines_schema = [
  {"name": "fl_date", "type": "STRING", "mode": "REQUIRED"},
  {"name": "op_carrier_airline_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "tail_num", "type": "STRING", "mode": "NULLABLE"},
  {"name": "op_carrier_fl_num", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "origin_airport_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "origin_airport_seq_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "origin_city_market_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "origin", "type": "STRING", "mode": "NULLABLE"},
  {"name": "dest_airport_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dest_airport_seq_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dest_city_market_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dest", "type": "STRING", "mode": "NULLABLE"},
  {"name": "crs_dep_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dep_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dep_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "dep_delay_new", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "arr_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "arr_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "arr_delay_new", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "cancelled", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "cancellation_code", "type": "STRING", "mode": "NULLABLE"},
  {"name": "crs_elapsed_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "actual_elapsed_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "carrier_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "weather_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "nas_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "security_delay", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "late_aircraft_delay", "type": "INTEGER", "mode": "NULLABLE"},
]

airlines_raw = TriggerDagRunOperator(
    task_id="airlines_raw",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": airlines_table_name, "file_name": airlines_file_name, \
           "delimiter": airlines_delimiter, "skip_leading_rows": airlines_skip_leading_rows, \
           "schema_full": airlines_schema_full, "schema": airlines_schema},
    dag=dag)


# load meals
meals_table_name = 'meals'
meals_file_name = 'initial_load/meals.csv'
meals_delimiter = "|"
meals_skip_leading_rows = 1

meals_schema_full = [
  {"name": "meal_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "meal_name", "type": "STRING", "mode": "REQUIRED"},
  {"name": "meal_image", "type": "STRING", "mode": "NULLABLE"},
  {"name": "cat_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "tags", "type": "STRING", "mode": "NULLABLE"},
  {"name": "area", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredient1", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredient2", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredient3", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredient4", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredient5", "type": "STRING", "mode": "NULLABLE"},
  {"name": "source", "type": "STRING", "mode": "NULLABLE"},
  {"name": "youtube", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

meals_schema = [
  {"name": "meal_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "meal_name", "type": "STRING", "mode": "REQUIRED"},
  {"name": "meal_image", "type": "STRING", "mode": "NULLABLE"},
  {"name": "cat_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "tags", "type": "STRING", "mode": "NULLABLE"},
  {"name": "area", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredient1", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredient2", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredient3", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredient4", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredient5", "type": "STRING", "mode": "NULLABLE"},
  {"name": "source", "type": "STRING", "mode": "NULLABLE"},
  {"name": "youtube", "type": "STRING", "mode": "NULLABLE"},
]

meals_raw = TriggerDagRunOperator(
    task_id="meals_raw",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": meals_table_name, "file_name": meals_file_name, \
           "delimiter": meals_delimiter, "skip_leading_rows": meals_skip_leading_rows, \
           "schema_full": meals_schema_full, "schema": meals_schema},
    dag=dag)


# load snacks
snacks_table_name = 'snacks'
snacks_file_name = 'initial_load/snacks.csv'
snacks_delimiter = ","
snacks_skip_leading_rows = 1

snacks_schema_full = [
  {"name": "code", "type": "STRING", "mode": "REQUIRED"},
  {"name": "url", "type": "STRING", "mode": "NULLABLE"},
  {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "brands", "type": "STRING", "mode": "NULLABLE"},
  {"name": "categories", "type": "STRING", "mode": "NULLABLE"},
  {"name": "countries_en", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredients_text", "type": "STRING", "mode": "NULLABLE"},
  {"name": "image_url", "type": "STRING", "mode": "NULLABLE"},
  {"name": "created_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "last_modified_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

snacks_schema = [
  {"name": "code", "type": "STRING", "mode": "REQUIRED"},
  {"name": "url", "type": "STRING", "mode": "NULLABLE"},
  {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "brands", "type": "STRING", "mode": "NULLABLE"},
  {"name": "categories", "type": "STRING", "mode": "NULLABLE"},
  {"name": "countries_en", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingredients_text", "type": "STRING", "mode": "NULLABLE"},
  {"name": "image_url", "type": "STRING", "mode": "NULLABLE"},
  {"name": "created_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "last_modified_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
]

snacks_raw = TriggerDagRunOperator(
    task_id="snacks_raw",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": snacks_table_name, "file_name": snacks_file_name, \
           "delimiter": snacks_delimiter, "skip_leading_rows": snacks_skip_leading_rows, \
           "schema_full": snacks_schema_full, "schema": snacks_schema},
    dag=dag)


ingest_start >> create_dataset_raw >> air_carriers_raw >> ingest_end
ingest_start >> create_dataset_raw >> bird_airports_raw >> ingest_end
ingest_start >> create_dataset_raw >> faker_airports_raw >> ingest_end
ingest_start >> create_dataset_raw >> airlines_raw >> ingest_end
ingest_start >> create_dataset_raw >> meals_raw >> ingest_end
ingest_start >> create_dataset_raw >> snacks_raw >> ingest_end

