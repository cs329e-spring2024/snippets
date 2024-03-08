import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

project_id = 'cs329e-sp2024'
stg_dataset_name = 'airline_stg_af'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='p7-key-controller-v0',
    default_args=default_args,
    description='key controller dag',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
join = DummyOperator(task_id="join", dag=dag)


# Air_Carrier PK
air_carrier_pk = f"alter table {stg_dataset_name}.Air_Carrier add primary key (airline_id) not enforced"
    
create_air_carrier_pk = BigQueryInsertJobOperator(
    task_id="create_air_carrier_pk",
    configuration={
        "query": {
            "query": air_carrier_pk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Flight PK
flight_pk = f"alter table {stg_dataset_name}.Flight add primary key (fl_num) not enforced"

create_flight_pk = BigQueryInsertJobOperator(
    task_id="create_flight_pk",
    configuration={
        "query": {
            "query": flight_pk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Flight_History PK
flight_history_pk = f"alter table {stg_dataset_name}.Flight_History add primary key (fl_date, fl_num) not enforced"

create_flight_history_pk = BigQueryInsertJobOperator(
    task_id="create_flight_history_pk",
    configuration={
        "query": {
            "query": flight_history_pk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Airport PK
create_airport_pk = f"alter table {stg_dataset_name}.Airport add primary key (iata) not enforced"

create_airport_pk = BigQueryInsertJobOperator(
    task_id="create_airport_pk",
    configuration={
        "query": {
            "query": create_airport_pk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

    
# Meal PK
meal_pk = f"alter table {stg_dataset_name}.Meal add primary key (meal_id) not enforced"

create_meal_pk = BigQueryInsertJobOperator(
    task_id="create_meal_pk",
    configuration={
        "query": {
            "query": meal_pk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
    
    
# Snack PK
snack_pk = f"alter table {stg_dataset_name}.Snack add primary key (snack_id) not enforced"

create_snack_pk = BigQueryInsertJobOperator(
    task_id="create_snack_pk",
    configuration={
        "query": {
            "query": snack_pk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Meal_Service PK
meal_service_pk = f"alter table {stg_dataset_name}.Meal_Service add primary key (fl_num, meal_id) not enforced"

create_meal_service_pk = BigQueryInsertJobOperator(
    task_id="create_meal_service_pk",
    configuration={
        "query": {
            "query": meal_service_pk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Snack_Service PK
snack_service_pk = f"alter table {stg_dataset_name}.Snack_Service add primary key (fl_num, snack_id) not enforced"

create_snack_service_pk = BigQueryInsertJobOperator(
    task_id="create_snack_service_pk",
    configuration={
        "query": {
            "query": snack_service_pk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
    

# Flight FK
flight_fk = f"""
alter table {stg_dataset_name}.Flight add foreign key (op_carrier_airline_id) 
references {stg_dataset_name}.Air_Carrier (airline_id) not enforced   
"""

create_flight_fk = BigQueryInsertJobOperator(
    task_id="create_flight_fk",
    configuration={
        "query": {
            "query": flight_fk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
  
    
# Flight_History FK
flight_history_fk = f"""
alter table {stg_dataset_name}.Flight_History add foreign key (fl_num) 
references {stg_dataset_name}.Flight (fl_num) not enforced   
"""

create_flight_history_fk = BigQueryInsertJobOperator(
    task_id="create_flight_history_fk",
    configuration={
        "query": {
            "query": flight_history_fk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Meal_Service FK #1
meal_service_m_fk = f"""
alter table {stg_dataset_name}.Meal_Service add foreign key (meal_id) 
references {stg_dataset_name}.Meal (meal_id) not enforced   
"""

create_meal_service_m_fk = BigQueryInsertJobOperator(
    task_id="create_meal_service_m_fk",
    configuration={
        "query": {
            "query": meal_service_m_fk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Meal_Service FK #2
meal_service_fl_fk = f"""
alter table {stg_dataset_name}.Meal_Service add foreign key (fl_num) 
references {stg_dataset_name}.Flight (fl_num) not enforced   
"""

create_meal_service_fl_fk = BigQueryInsertJobOperator(
    task_id="create_meal_service_fl_fk",
    configuration={
        "query": {
            "query": meal_service_fl_fk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Meal_Service FK #3
meal_service_al_fk = f"""
alter table {stg_dataset_name}.Meal_Service add foreign key (airline_id) 
references {stg_dataset_name}.Air_Carrier (airline_id) not enforced   
"""

create_meal_service_al_fk = BigQueryInsertJobOperator(
    task_id="create_meal_service_al_fk",
    configuration={
        "query": {
            "query": meal_service_al_fk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
    

# Snack_Service FK #1
snack_service_s_fk = f"""
alter table {stg_dataset_name}.Snack_Service add foreign key (snack_id) 
references {stg_dataset_name}.Snack (snack_id) not enforced   
"""

create_snack_service_s_fk = BigQueryInsertJobOperator(
    task_id="create_snack_service_s_fk",
    configuration={
        "query": {
            "query": snack_service_s_fk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Snack_Service FK #2
snack_service_fl_fk = f"""
alter table {stg_dataset_name}.Snack_Service add foreign key (fl_num) 
references {stg_dataset_name}.Flight (fl_num) not enforced   
"""

create_snack_service_fl_fk = BigQueryInsertJobOperator(
    task_id="create_snack_service_fl_fk",
    configuration={
        "query": {
            "query": snack_service_fl_fk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Meal_Service FK #3
snack_service_al_fk = f"""
alter table {stg_dataset_name}.Snack_Service add foreign key (airline_id) 
references {stg_dataset_name}.Air_Carrier (airline_id) not enforced   
"""

create_snack_service_al_fk = BigQueryInsertJobOperator(
    task_id="create_snack_service_al_fk",
    configuration={
        "query": {
            "query": snack_service_al_fk,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

    

# Primary Keys
start >> create_air_carrier_pk >> join
start >> create_flight_pk >> join
start >> create_flight_history_pk >> join
start >> create_airport_pk >> join
start >> create_meal_pk >> join
start >> create_snack_pk >> join
start >> create_meal_service_pk >> join
start >> create_snack_service_pk >> join


# Foreign Keys
join >> create_flight_fk >> end
join >> create_flight_history_fk >> end
join >> create_meal_service_m_fk >> end
join >> create_meal_service_fl_fk >> end
join >> create_meal_service_al_fk >> end
join >> create_snack_service_s_fk >> end
join >> create_snack_service_fl_fk >> end
join >> create_snack_service_al_fk >> end
