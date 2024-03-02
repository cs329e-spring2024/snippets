"""A dag that creates the bird_airports table in the staging layer."""
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


@task(trigger_rule="all_done")
def _create_bird_airports(**kwargs):
    
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    raw_dataset_name = kwargs["dag_run"].conf["raw_dataset_name"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]
    
    raw_table_name = "bird_airports"
    stg_table_name = "bird_airports" # lowercase the name because it's an intermediate table
    
    bird_airports = []
    target_table_id = "{}.{}.{}".format(project_id, stg_dataset_name, stg_table_name)

    schema = [bigquery.SchemaField("code", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
              bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
              bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
              bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
              bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("load_time", "TIMESTAMP", mode="REQUIRED")] # don't propagate current_timestamp(), this rule should 
                                                                               # only be set in the raw tables

    bq_client = bigquery.Client(project=project_id, location=region)
    sql = "select code, description, load_time from {}.{}".format(raw_dataset_name, raw_table_name)
    query_job = bq_client.query(sql)

    index = 0

    for row in query_job:
        code = row["code"]
        description = row["description"]
        load_time = json.dumps(row["load_time"], default=serialize_datetime).replace('"', '')
        city = description.split(",")[0].strip()

        if len(description.split(",")) > 1:
            state_country = description.split(",")[1].split(":")[0].strip()

            if state_country.isupper() and len(state_country) == 2:
                state = state_country
                country = 'US'
            else:
                state = None
                country = state_country

        else:
            state_country = None
            print('state_country is null: ' + description)

        if len(description.split(":")) > 1:
            name = description.split(":")[1].strip()
        else:
            name = None
            print('airport name is null: ' + description)

        record = {"code": code, "name": name, "city": city, "state": state, "country": country, "data_source": "bird", "load_time": load_time}
        filtered_record = remove_none_values(record)

        if index == 0:
            print('printing first record for debugging', filtered_record)

        bird_airports.append(filtered_record)

        index += 1

    # load the records into staging table
    job_config = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition='WRITE_TRUNCATE')
    table_ref = bigquery.table.TableReference.from_string(target_table_id)

    try:
        load_job = bq_client.load_table_from_json(bird_airports, table_ref, job_config=job_config)
        load_job.result()
        destination_table = bq_client.get_table(target_table_id)
        print('Wrote', destination_table.num_rows, 'records into', target_table_id)

        if load_job.errors:
          print('job errors:', load_job.errors)

    except Exception as e:
        print("Error inserting into BQ: {}".format(e))
    
    
with models.DAG(
    "p6-create-bird-airports",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    create_bird_airports = _create_bird_airports()

    # task dependencies
    create_bird_airports
