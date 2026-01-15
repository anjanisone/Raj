import datetime
from airflow import airflow
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from jinja2 import Template

from lib.common_lib import read_storage_object_as_string, execute_query

DAG_NAME = "wf_common_adhoc_sql_file_execution_with_audit"
GCP_PROJECT = Variable.get("GCP_PROJECT")

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
    tags=["adhoc", "sql"],
    params={
        "sql_path": "",
        "query_params": {},
        "dry_run": False
    }
)

def execute_sql(**kwargs):
    params = kwargs["dag_run"].conf

    if not params.get("sql_path"):
        raise ValueError("sql_path is required")

    raw_sql = read_storage_object_as_string(params["sql_path"])
    final_sql = Template(raw_sql).render(**params.get("query_params", {}))

    bq_client = bigquery.Client(project=GCP_PROJECT)
    start_ts = datetime.datetime.now()

    query_job = execute_query(
        final_sql,
        True,
        params.get("dry_run", False),
        "airflow_adhoc_sql_"
    )

    query_job.result()

    audit_row = [{
        "Start_Ts": str(start_ts),
        "End_Ts": str(datetime.datetime.now()),
        "Sql": final_sql,
        "Total_Rows_Affected": query_job.num_dml_affected_rows,
        "Total_Bytes_Processed": query_job.total_bytes_processed,
        "Total_Bytes_Billed": query_job.total_bytes_billed,
        "BQ_Job_Id": query_job.job_id,
        "Dry_Run": query_job.dry_run
    }]

    errors = bq_client.insert_rows_json(
        "EDAP_CNTL.Adhoc_Sql_Audit",
        audit_row
    )

    if errors:
        raise RuntimeError(errors)

EXECUTE_SQL = PythonOperator(
    task_id="EXECUTE_SQL",
    python_callable=execute_sql,
    dag=dag
)
