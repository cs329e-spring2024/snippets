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
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='p7-key-controller',
    default_args=default_args,
    description='key controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
join = DummyOperator(task_id="join", dag=dag)


# Air_Carrier PK
table_name = "Air_Carrier"
pk_columns = ["airline_id"]

air_carrier_pk = TriggerDagRunOperator(
    task_id="air_carrier_pk",
    trigger_dag_id="p7-create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)


# Flight PK
table_name = "Flight"
pk_columns = ["fl_num"]

flight_pk = TriggerDagRunOperator(
    task_id="flight_pk",
    trigger_dag_id="p7-create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)


# Flight_History PK
table_name = "Flight_History"
pk_columns = ["fl_date", "fl_num"]

flight_history_pk = TriggerDagRunOperator(
    task_id="flight_history_pk",
    trigger_dag_id="p7-create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
    
    
# Airport PK
table_name = "Airport"
pk_columns = ["iata"]

airport_pk = TriggerDagRunOperator(
    task_id="airport_pk",
    trigger_dag_id="p7-create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
  
    
# Meal PK
table_name = "Meal"
pk_columns = ["meal_id"]

meal_pk = TriggerDagRunOperator(
    task_id="meal_pk",
    trigger_dag_id="p7-create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
    

# Snack PK
table_name = "Snack"
pk_columns = ["snack_id"]

snack_pk = TriggerDagRunOperator(
    task_id="snack_pk",
    trigger_dag_id="p7-create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
    
    
# Meal_Service PK
table_name = "Meal_Service"
pk_columns = ["meal_id", "fl_num"]

meal_service_pk = TriggerDagRunOperator(
    task_id="meal_service_pk",
    trigger_dag_id="p7-create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
    

# Snack_Service PK
table_name = "Snack_Service"
pk_columns = ["snack_id", "fl_num"]

snack_service_pk = TriggerDagRunOperator(
    task_id="snack_service_pk",
    trigger_dag_id="p7-create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
    

# Flight FK #1
child_table_name = "Flight"
fk_columns = ["op_carrier_airline_id"]
parent_table_name = "Air_Carrier"
pk_columns = ["airline_id"]

flight_op_carrier_airline_id_fk = TriggerDagRunOperator(
    task_id="flight_op_carrier_airline_id_fk",
    trigger_dag_id="p7-create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
  
  
# Flight FK #2
child_table_name = "Flight"
fk_columns = ["origin_airport"]
parent_table_name = "Airport"
pk_columns = ["iata"]

flight_origin_airport_fk = TriggerDagRunOperator(
    task_id="flight_origin_airport_fk",
    trigger_dag_id="p7-create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
    

# Flight FK #3
child_table_name = "Flight"
fk_columns = ["dest_airport"]
parent_table_name = "Airport"
pk_columns = ["iata"]

flight_dest_airport_fk = TriggerDagRunOperator(
    task_id="flight_dest_airport_fk",
    trigger_dag_id="p7-create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
   
    
# Flight_History FK
child_table_name = "Flight_History"
fk_columns = ["fl_num"]
parent_table_name = "Flight"
pk_columns = ["fl_num"]

flight_history_fk = TriggerDagRunOperator(
    task_id="flight_history_fk",
    trigger_dag_id="p7-create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
   
    
# Meal_Service FK #1
child_table_name = "Meal_Service"
fk_columns = ["meal_id"]
parent_table_name = "Meal"
pk_columns = ["meal_id"]

meal_service_meal_id_fk = TriggerDagRunOperator(
    task_id="meal_service_meal_id_fk",
    trigger_dag_id="p7-create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
    
    
# Meal_Service FK #2
child_table_name = "Meal_Service"
fk_columns = ["fl_num"]
parent_table_name = "Flight"
pk_columns = ["fl_num"]

meal_service_fl_num_fk = TriggerDagRunOperator(
    task_id="meal_service_fl_num_fk",
    trigger_dag_id="p7-create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
    
    
# Meal_Service FK #3
child_table_name = "Meal_Service"
fk_columns = ["airline_id"]
parent_table_name = "Air_Carrier"
pk_columns = ["airline_id"]

meal_service_airline_id_fk = TriggerDagRunOperator(
    task_id="meal_service_airline_id_fk",
    trigger_dag_id="p7-create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)


# Snack_Service FK #1
child_table_name = "Snack_Service"
fk_columns = ["snack_id"]
parent_table_name = "Snack"
pk_columns = ["snack_id"]

snack_service_snack_id_fk = TriggerDagRunOperator(
    task_id="snack_service_snack_id_fk",
    trigger_dag_id="p7-create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
    
    
# Snack_Service FK #2
child_table_name = "Snack_Service"
fk_columns = ["fl_num"]
parent_table_name = "Flight"
pk_columns = ["fl_num"]

snack_service_fl_num_fk = TriggerDagRunOperator(
    task_id="snack_service_fl_num_fk",
    trigger_dag_id="p7-create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
    
    
# Snack_Service FK #3
child_table_name = "Snack_Service"
fk_columns = ["airline_id"]
parent_table_name = "Air_Carrier"
pk_columns = ["airline_id"]

snack_service_airline_id_fk = TriggerDagRunOperator(
    task_id="snack_service_airline_id_fk",
    trigger_dag_id="p7-create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)

    

# Primary Keys
start >> air_carrier_pk >> join
start >> flight_pk >> join
start >> flight_history_pk >> join
start >> airport_pk >> join
start >> meal_pk >> join
start >> snack_pk >> join
start >> meal_service_pk >> join
start >> snack_service_pk >> join

# Foreign Keys
join >> flight_op_carrier_airline_id_fk >> end
join >> flight_origin_airport_fk >> end
join >> flight_dest_airport_fk >> end
join >> flight_history_fk >> end
join >> meal_service_meal_id_fk >> end
join >> meal_service_fl_num_fk >> end
join >> meal_service_airline_id_fk >> end
join >> snack_service_snack_id_fk >> end
join >> snack_service_fl_num_fk >> end
join >> snack_service_airline_id_fk >> end
   