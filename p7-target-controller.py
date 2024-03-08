import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

project_id = 'cs329e-sp2024'
stg_dataset_name = 'airline_stg_af'
csp_dataset_name = 'airline_csp_af'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='p7-target-controller',
    default_args=default_args,
    description='target table controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)


# create csp dataset
create_dataset_csp = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_csp',
    project_id=project_id,
    dataset_id=csp_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# create Air_Carrier
air_carrier_sql = (
f"""
create or replace table {csp_dataset_name}.Air_Carrier(
  airline_id	INT64 not null,
  airline_name	STRING not null,
  airline_code	STRING,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (airline_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Air_Carrier
""")

create_air_carrier_csp = BigQueryInsertJobOperator(
    task_id="create_air_carrier_csp",
    configuration={
        "query": {
            "query": air_carrier_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Airport
airport_sql = (
f"""
create or replace table {csp_dataset_name}.Airport(
  iata STRING not null,
  icao STRING,
  name STRING,
  city STRING not null,
  state STRING,
  country STRING not null,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (iata, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Airport
"""
)

create_airport_csp = BigQueryInsertJobOperator(
    task_id="create_airport_csp",
    configuration={
        "query": {
            "query": airport_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Flight
flight_sql = (
f"""
create or replace table {csp_dataset_name}.Flight(
  fl_num STRING not null,
  op_carrier_airline_id INT64 not null,
  op_carrier_fl_num INT64 not null,
  origin_airport STRING not null,
  dest_airport STRING not null,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (fl_num, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Flight
"""
)

create_flight_csp = BigQueryInsertJobOperator(
    task_id="create_flight_csp",
    configuration={
        "query": {
            "query": flight_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Flight_History
flight_history_sql = (
f"""
create or replace table {csp_dataset_name}.Flight_History(
  fl_date DATE not null,
  fl_num STRING not null,
  tail_num STRING,
  crs_dep_time INT64 not null,
  dep_time INT64,
  dep_delay INT64,
  dep_delay_new INT64,
  arr_time INT64,
  arr_delay INT64,
  arr_delay_new INT64,
  cancelled INT64,
  cancellation_code STRING,
  crs_elapsed_time INT64,
  actual_elapsed_time INT64,
  carrier_delay INT64,
  weather_delay INT64,
  nas_delay INT64,
  security_delay INT64,
  late_aircraft_delay INT64,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (fl_date, fl_num, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Flight_History
"""
)

create_flight_history_csp = BigQueryInsertJobOperator(
    task_id="create_flight_history_csp",
    configuration={
        "query": {
            "query": flight_history_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

    
# create Meal
meal_sql = (
f"""
create or replace table {csp_dataset_name}.Meal(
  meal_id	INT64 not null,
  meal_name STRING not null,
  meal_image STRING,
  cat_name STRING,
  tags STRING,
  area STRING,
  ingredient1 STRING,
  ingredient2 STRING,
  ingredient3 STRING,
  ingredient4 STRING,
  ingredient5 STRING,
  source STRING,
  youtube STRING,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (meal_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Meal
"""
)

create_meal_csp = BigQueryInsertJobOperator(
    task_id="create_meal_csp",
    configuration={
        "query": {
            "query": meal_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
    

# create Meal_Service
meal_service_sql = (
f"""
create or replace table {csp_dataset_name}.Meal_Service(
  fl_num STRING not null,
  meal_id	INT64 not null,
  airline_id INT64 not null,
  service_category STRING,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (fl_num, meal_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Meal_Service
"""
)

create_meal_service_csp = BigQueryInsertJobOperator(
    task_id="create_meal_service_csp",
    configuration={
        "query": {
            "query": meal_service_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Snack
snack_sql = (
f"""
create or replace table {csp_dataset_name}.Snack(
  snack_id STRING not null,
  url	STRING not null,
  product_name STRING,
  brands STRING,
  categories STRING not null,
  countries_en STRING,
  ingredients_text STRING,
  image_url	STRING,
  created_time TIMESTAMP not null,
  last_modified_time TIMESTAMP not null,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (snack_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Snack
"""
)

create_snack_csp = BigQueryInsertJobOperator(
    task_id="create_snack_csp",
    configuration={
        "query": {
            "query": snack_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
    
    
# create Snack_Service
snack_service_sql = (
f"""
create or replace table {csp_dataset_name}.Snack_Service(
  fl_num STRING not null,
  snack_id STRING not null,
  airline_id INT64 not null,
  service_category STRING,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (fl_num, snack_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Snack_Service
"""
)

create_snack_service_csp = BigQueryInsertJobOperator(
    task_id="create_snack_service_csp",
    configuration={
        "query": {
            "query": snack_service_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


start >> create_dataset_csp >> create_air_carrier_csp >> end
start >> create_dataset_csp >> create_airport_csp >> end
start >> create_dataset_csp >> create_flight_csp >> end
start >> create_dataset_csp >> create_flight_history_csp >> end
start >> create_dataset_csp >> create_meal_csp >> end
start >> create_dataset_csp >> create_meal_service_csp >> end
start >> create_dataset_csp >> create_snack_csp >> end
start >> create_dataset_csp >> create_snack_service_csp >> end

