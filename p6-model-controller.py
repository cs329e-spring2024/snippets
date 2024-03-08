import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

project_id = 'cs329e-sp2024'
raw_dataset_name = 'airline_raw_af'
stg_dataset_name = 'airline_stg_af'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='p6-model-controller',
    default_args=default_args,
    description='model controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
join = DummyOperator(task_id="join", dag=dag)
wait = TimeDeltaSensor(task_id="wait", delta=duration(seconds=5), dag=dag)

# create stg dataset
create_dataset_stg = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_stg',
    project_id=project_id,
    dataset_id=stg_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# create Air_Carrier
# Note: don't propagate default current_timestamp() on the load_time column, 
# this rule should only be applied to the raw tables
air_carrier_sql = (
f"""create or replace table {stg_dataset_name}.Air_Carrier(
    airline_id INT64 not null,
    airline_name STRING not null,
    airline_code STRING not null,
    data_source STRING not null,
    load_time TIMESTAMP not null)
    as
      select airline_id, description_array[0] as airline_name, description_array[1] as airline_code, 
          'bird' as data_source, load_time
      from
          (select code as airline_id, split(description, ':') as description_array, load_time
          from {raw_dataset_name}.air_carriers)""")

create_air_carrier_stg = BigQueryInsertJobOperator(
    task_id="create_air_carrier_stg",
    configuration={
        "query": {
            "query": air_carrier_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create bird_airports (intermediate)
create_bird_airports_stg = TriggerDagRunOperator(
    task_id="create_bird_airports_stg",
    trigger_dag_id="p6-create-bird-airports",  
    conf={"project_id": project_id, "region": region, "raw_dataset_name": raw_dataset_name, "stg_dataset_name": stg_dataset_name},
    dag=dag)

# create airlines (intermediate)
airlines_sql = (
f"""
create or replace table {stg_dataset_name}.airlines as
  select concat(op_carrier_airline_id, op_carrier_fl_num, origin, dest) as fl_num,
      op_carrier_airline_id,
      op_carrier_fl_num,
      origin_airport_id,
      origin_airport_seq_id,
      origin_city_market_id,
      origin,
      dest_airport_id,
      dest_airport_seq_id,
      dest_city_market_id,
      dest,
      fl_date,
      tail_num,
      crs_dep_time,
      dep_time,
      dep_delay,
      dep_delay_new,
      arr_time,
      arr_delay,
      arr_delay_new,
      cancelled,
      cancellation_code,
      crs_elapsed_time,
      actual_elapsed_time,
      carrier_delay,
      weather_delay,
      nas_delay,
      security_delay,
      late_aircraft_delay,
      'bird' as data_source,
      load_time
  from {raw_dataset_name}.airlines
""")

create_airlines_stg = BigQueryInsertJobOperator(
    task_id="create_airlines_stg",
    configuration={
        "query": {
            "query": airlines_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Flight
flight_sql = (
f"""
create or replace table {stg_dataset_name}.Flight( 
    fl_num STRING not null,
    op_carrier_airline_id INTEGER not null,
    op_carrier_fl_num INTEGER not null,
    origin_airport STRING not null,
    dest_airport STRING not null,
    data_source STRING not null,
    load_time TIMESTAMP not null)
    as
      select distinct fl_num,
          op_carrier_airline_id,
          op_carrier_fl_num,
          origin as origin_airport,
          dest as dest_airport,
          data_source,
          load_time
      from {stg_dataset_name}.airlines
"""
)

create_flight_stg = BigQueryInsertJobOperator(
    task_id="create_flight_stg",
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
create or replace table {stg_dataset_name}.Flight_History(
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
    load_time TIMESTAMP not null) 
    as
        select date(safe_cast(split(fl_date, '/')[0] as int),
            safe_cast(split(fl_date, '/')[1] as int),
            safe_cast(split(fl_date, '/')[2] as int)) as fl_date,
            fl_num,
            tail_num,
            crs_dep_time,
            dep_time,
            dep_delay,
            dep_delay_new,
            arr_time,
            arr_delay,
            arr_delay_new,
            cancelled,
            cancellation_code,
            crs_elapsed_time,
            actual_elapsed_time,
            carrier_delay,
            weather_delay,
            nas_delay,
            security_delay,
            late_aircraft_delay,
            data_source,
            load_time
         from {stg_dataset_name}.airlines
"""
)

create_flight_history_stg = BigQueryInsertJobOperator(
    task_id="create_flight_history_stg",
    configuration={
        "query": {
            "query": flight_history_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# delete airlines (intermediate table)
delete_airlines_stg = BigQueryDeleteTableOperator(
    task_id="delete_airlines_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.airlines",
    location=region,
    dag=dag)

# create Airport
airport_sql = (
f"""
create or replace table {stg_dataset_name}.Airport( 
    iata STRING not null,
    icao STRING,
    name STRING,
    city STRING not null,
    state STRING,
    country STRING not null,
    data_source STRING not null,
    load_time TIMESTAMP)
    as
        select b.code as iata, f.icao as icao,
        b.name, b.city, b.state, b.country, b.data_source, b.load_time
        from {stg_dataset_name}.bird_airports b
        left join {raw_dataset_name}.faker_airports f
        on b.code = f.iata
"""
)

create_airport_stg = BigQueryInsertJobOperator(
    task_id="create_airport_stg",
    configuration={
        "query": {
            "query": airport_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# update data_source in Airport    
data_source_sql = (
f"""
    update {stg_dataset_name}.Airport set data_source = 'bird_faker' where icao is not null
"""
)

update_data_source = BigQueryInsertJobOperator(
    task_id="update_data_source",
    configuration={
        "query": {
            "query": data_source_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# update state in Airport    
state_sql = (
f"""
    update {stg_dataset_name}.Airport a set state =
        (select distinct state
         from {raw_dataset_name}.faker_airports f
         where f.iata = a.iata
         and f.state is not null), data_source = 'bird_faker'
      where state is null
"""
)

update_state = BigQueryInsertJobOperator(
    task_id="update_state",
    configuration={
        "query": {
            "query": state_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# delete bird_airports (intermediate table)
delete_bird_airports_stg = BigQueryDeleteTableOperator(
    task_id="delete_bird_airports_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.bird_airports",
    location=region,
    dag=dag)
    
# create Meal
meal_sql = (
f"""
create or replace table {stg_dataset_name}.Meal(
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
    load_time TIMESTAMP not null) 
    as
      select distinct * except(load_time), 'mealdb' as data_source, load_time
      from {raw_dataset_name}.meals
"""
)

create_meal_stg = BigQueryInsertJobOperator(
    task_id="create_meal_stg",
    configuration={
        "query": {
            "query": meal_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
    
# create snacks (intermediate table)
snacks_sql = (
f"""
create or replace table {stg_dataset_name}.snacks as
  select code as snack_id, * except(code, rank, load_time), 'open_food_facts' as data_source, load_time
  from
    (select RANK() over (partition by code order by last_modified_datetime desc) AS rank, *
    from {raw_dataset_name}.snacks)
  where rank = 1
"""
)

create_snacks_stg = BigQueryInsertJobOperator(
    task_id="create_snacks_stg",
    configuration={
        "query": {
            "query": snacks_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Snack
snack_sql = (
f"""
create or replace table {stg_dataset_name}.Snack(
    snack_id STRING not null,
    url	STRING not null,
    product_name STRING,
    brands STRING,
    categories STRING not null,
    countries_en STRING,
    ingredients_text STRING,
    image_url STRING,
    created_time TIMESTAMP not null,
    last_modified_time TIMESTAMP not null,
    data_source	STRING not null,
    load_time TIMESTAMP not null) 
    as
      select snack_id, url, product_name, brands, categories, countries_en, ingredients_text, image_url,
        created_datetime as created_time, last_modified_datetime as last_modified_time, data_source, load_time
      from
        (select RANK() over (partition by snack_id order by categories desc) AS rank, *
            from {stg_dataset_name}.snacks)
      where rank = 1
"""
)

create_snack_stg = BigQueryInsertJobOperator(
    task_id="create_snack_stg",
    configuration={
        "query": {
            "query": snack_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# delete snacks (intermediate table)
delete_snacks_stg = BigQueryDeleteTableOperator(
    task_id="delete_snacks_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.snacks",
    location=region,
    dag=dag)

# create Meal_Service and Snack_Service 
create_meal_snack_service_stg = TriggerDagRunOperator(
    task_id="create_meal_snack_service_stg",
    trigger_dag_id="p6-create-meal-snack-service",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name},
    dag=dag)

start >> create_dataset_stg >> create_air_carrier_stg >> end
start >> create_dataset_stg >> create_bird_airports_stg >> wait >> create_airport_stg >> update_data_source >> update_state >> delete_bird_airports_stg >> end
start >> create_dataset_stg >> create_airlines_stg >> create_flight_stg >> create_flight_history_stg >> delete_airlines_stg >> end
start >> create_dataset_stg >> create_meal_stg >> join
start >> create_dataset_stg >> create_snacks_stg >> create_snack_stg >> delete_snacks_stg >> join
join >> create_meal_snack_service_stg >> end

