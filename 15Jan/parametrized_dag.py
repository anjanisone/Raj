import datetime
from airflow import airflow
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from jinja2 import Template

from lib.common_lib import read_storage_object_as_string, execute_query

DAG_NAME = "wf_common_adhoc_sql_runner"

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 0,
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["adhoc"]
)

def execute_sql(**kwargs):
    sql_path = Variable.get("ADHOC_SQL_PATH")
    job_name = Variable.get("ADHOC_JOB_NAME", default_var=DAG_NAME)
    dry_run = Variable.get("ADHOC_DRY_RUN", default_var="false").lower() == "true"

    raw_sql = read_storage_object_as_string(sql_path)
    final_sql = Template(raw_sql).render()

    bq_client = bigquery.Client()
    start_ts = datetime.datetime.now()

    query_job = execute_query(
        final_sql,
        True,
        dry_run,
        f"airflow_{job_name}_"
    )

    query_job.result()

    bq_client.insert_rows_json(
        "EDAP_CNTL.Adhoc_Sql_Audit",
        [{
            "Start_Ts": str(start_ts),
            "End_Ts": str(datetime.datetime.now()),
            "Sql": final_sql,
            "BQ_Job_Id": query_job.job_id,
            "Dry_Run": query_job.dry_run
        }]
    )

EXECUTE_SQL = PythonOperator(
    task_id="EXECUTE_SQL",
    python_callable=execute_sql,
    dag=dag
)
