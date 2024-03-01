"""A dag that ingests all the raw data into BQ."""
import airflow
from airflow import models
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
import json, datetime

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

# removes the entries from the dictionary whose values are None
# this filter is needed for loading JSON into BQ
def remove_none_values(record):
    filtered_record = {}
    for field in record.keys():
        if record[field] != None:
            filtered_record[field] = record[field]
    return filtered_record


def create_table(bq_client, table_id, schema):
    
    table = bigquery.Table(table_id, schema=schema)
    table = bq_client.create_table(table, exists_ok=True)
    print("Created table {}".format(table.table_id))


def load_records(bq_client, table_id, schema, records):

    print('inside load_records: table: ', table_id, ' num records:', str(len(records)))
    print('schema:', schema)
    print('sample record:', records[0])
    
    # load records into staging table
    job_config = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition='WRITE_TRUNCATE')
    table_ref = bigquery.table.TableReference.from_string(table_id)

    try:
        load_job = bq_client.load_table_from_json(records, table_ref, job_config=job_config)
        load_job.result()

        destination_table = bq_client.get_table(target_table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))

        if load_job.errors:
            print('job errors:', load_job.errors)


    except Exception as e:
        print("Error inserting into BQ: {}".format(e))


@task(trigger_rule="all_done")
def _create_meal_service(**kwargs):
    
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]
    
    ms_records = [] # meal service records to be inserted
    meal_ids = []

    stg_table_name = "Meal_Service" # uppercase the name because it's a final table
    target_table_id = f"{project_id}.{stg_dataset_name}.{stg_table_name}"

    schema = [bigquery.SchemaField("fl_num", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("meal_id", "INTEGER", mode="REQUIRED"),
              bigquery.SchemaField("airline_id", "INTEGER", mode="NULLABLE"),
              bigquery.SchemaField("service_category", "STRING", mode="NULLABLE"),
              bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("load_time", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP"),
              ]

    bq_client = bigquery.Client(project=project_id, location=region)
    create_table(bq_client, target_table_id, schema)

    # now that Meal_Service table has been created, remove the load_time field from the schema
    # because we expect BQ to auto-populate it and we are not including it in the JSON record
    del schema[-1]

    meal_sql = f"select meal_id from {stg_dataset_name}.Meal"
    query_job = bq_client.query(meal_sql)

    # add the meal ids to a list
    for row in query_job:
        meal_ids.append(row["meal_id"])

    # get all the long flights grouped by airline
    flight_sql = f"""
    select op_carrier_airline_id, array_agg(fl_num) as fl_numbers
    from
      (select op_carrier_airline_id, fl_num
        from
        (select f.fl_num, f.op_carrier_airline_id
          from {stg_dataset_name}.Flight f join {stg_dataset_name}.Flight_History fh
          on f.fl_num = fh.fl_num
          where fh.crs_elapsed_time >= 8
          order by fh.crs_elapsed_time desc)
        group by op_carrier_airline_id, fl_num
        order by op_carrier_airline_id)
    group by op_carrier_airline_id
    """

    query_job = bq_client.query(flight_sql)

    index = 0

    # for each airline
    for row in query_job:
        airline_id = row["op_carrier_airline_id"]
        fl_numbers = row["fl_numbers"]

        # there are no more meals to allocate
        if index >= len(meal_ids):
            break

        meal_id = meal_ids[index]

        # assign a meal to all of the airline's "long" flights
        for fl_num in fl_numbers:
            record = {"fl_num": fl_num, "meal_id": meal_id, "airline_id": airline_id, "data_source": "bird_mealdb"}

            filtered_record = remove_none_values(record)

            ms_records.append(filtered_record)

        index += 1

    # insert records into the Meal_Service table
    load_records(bq_client, target_table_id, schema, ms_records)
    

# Snack_Service
@task(trigger_rule="all_done")
def _create_snack_service(**kwargs):
    
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]
    stg_table_name = "Snack_Service" # uppercase the name because it's a final table
    
    ss_records = [] # snack service records to be inserted
    snack_ids = []
    
    target_table_id = f"{project_id}.{stg_dataset_name}.{stg_table_name}"

    schema = [bigquery.SchemaField("fl_num", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("snack_id", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("airline_id", "INTEGER", mode="NULLABLE"),
              bigquery.SchemaField("service_category", "STRING", mode="NULLABLE"),
              bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("load_time", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP"),
              ]
    
    bq_client = bigquery.Client(project=project_id, location=region)
    create_table(bq_client, target_table_id, schema)

    # now that Snack_Service table has been created, remove the load_time field from the schema
    # because we expect BQ to auto-populate it and we are not including it in the JSON record
    del schema[-1]

    snack_sql = f"select snack_id from {stg_dataset_name}.Snack where countries_en like '%United States%'"
    query_job = bq_client.query(snack_sql)

    # add the snack ids to a list
    for row in query_job:
        snack_ids.append(row["snack_id"])

    # get the list of US flights grouped by airline
    flight_sql = f"""
    select f.op_carrier_airline_id, ARRAY_AGG(f.fl_num) as fl_numbers
    from {stg_dataset_name}.Flight f join {stg_dataset_name}.Airport a
    on f.origin_airport = a.iata
    where country = 'US'
    group by f.op_carrier_airline_id
    """

    query_job = bq_client.query(flight_sql)
    
    index = 0

    # for each airline and its US flights
    for row in query_job:
        airline_id = row["op_carrier_airline_id"]
        fl_numbers = row["fl_numbers"]

        # there are no more meals to allocate
        if index >= len(snack_ids):
            break

        snack_id = snack_ids[index]

        # assign a snack to all of thid airline's US flights
        for fl_num in fl_numbers:
            record = {"fl_num": fl_num, "snack_id": snack_id, "airline_id": airline_id, "data_source": "bird_open_food_facts"}

            filtered_record = remove_none_values(record)
            ss_records.append(filtered_record)

        index += 1

    # insert records into the Snack_Service table
    load_records(bq_client, target_table_id, schema, ss_records)  
  
    
with models.DAG(
    "p6-create-meal-snack-service",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    create_meal_service = _create_meal_service()
    create_snack_service = _create_snack_service()

    # run both tasks in parallel
    create_meal_service
    create_snack_service
