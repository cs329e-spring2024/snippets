import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='p7-master-controller',
    default_args=default_args,
    description='master controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
wait_after_ingest = TimeDeltaSensor(task_id="wait_after_ingest", delta=duration(seconds=60), dag=dag)
wait_after_model = TimeDeltaSensor(task_id="wait_after_model", delta=duration(seconds=180), dag=dag)
wait_after_key = TimeDeltaSensor(task_id="wait_after_key", delta=duration(seconds=120), dag=dag)

# ingest controller
ingest_controller = TriggerDagRunOperator(
    task_id="ingest_controller",
    trigger_dag_id="p5-ingest-controller",  
    dag=dag)
    
# model controller
model_controller = TriggerDagRunOperator(
    task_id="model_controller",
    trigger_dag_id="p6-model-controller",  
    dag=dag)
    
# primary key and foreign key controller
key_controller = TriggerDagRunOperator(
    task_id="key_controller",
    trigger_dag_id="p7-key-controller",  
    dag=dag)

# target table controller
target_controller = TriggerDagRunOperator(
    task_id="target_controller",
    trigger_dag_id="p7-target-controller",  
    dag=dag)

start >> ingest_controller >> wait_after_ingest >> model_controller >> wait_after_model >> key_controller >> wait_after_key >> target_controller >> end


