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
    tags=["adhoc", "resumable"]
)

def get_last_success_key(bq_client, job_name):
    sql = f"""
        SELECT COALESCE(MAX(last_success_key), '0') AS last_key
        FROM EDAP_CNTL.SQL_EXECUTION_CHECKPOINT
        WHERE job_name = '{job_name}'
          AND status = 'SUCCESS'
    """
    rows = bq_client.query(sql).result()
    return list(rows)[0]["last_key"]

def write_checkpoint(
    bq_client,
    job_name,
    run_id,
    batch_no,
    last_success_key,
    rows_processed,
    status,
    error_message=None
):
    bq_client.insert_rows_json(
        "EDAP_CNTL.SQL_EXECUTION_CHECKPOINT",
        [{
            "job_name": job_name,
            "run_id": run_id,
            "batch_no": batch_no,
            "last_success_key": str(last_success_key),
            "rows_processed": rows_processed,
            "status": status,
            "start_ts": datetime.datetime.now().isoformat(),
            "end_ts": datetime.datetime.now().isoformat(),
            "error_message": error_message
        }]
    )

def execute_sql(**kwargs):
    sql_path = Variable.get("ADHOC_SQL_PATH")
    job_name = Variable.get("ADHOC_JOB_NAME", default_var=DAG_NAME)
    dry_run = Variable.get("ADHOC_DRY_RUN", default_var="false").lower() == "true"
    batch_size = int(Variable.get("ADHOC_BATCH_SIZE", default_var="500000"))

    if not sql_path:
        raise ValueError("ADHOC_SQL_PATH is required")

    bq_client = bigquery.Client()
    run_id = kwargs["run_id"]

    sql_template = read_storage_object_as_string(sql_path)
    last_success_key = get_last_success_key(bq_client, job_name)

    batch_no = 1

    while True:
        rendered_sql = Template(sql_template).render(
            last_success_key=last_success_key,
            batch_size=batch_size
        )

        try:
            start_ts = datetime.datetime.now()

            query_job = execute_query(
                rendered_sql,
                True,
                dry_run,
                f"airflow_{job_name}_"
            )

            query_job.result()
            rows = query_job.num_dml_affected_rows or 0

            bq_client.insert_rows_json(
                "EDAP_CNTL.Adhoc_Sql_Audit",
                [{
                    "Start_Ts": str(start_ts),
                    "End_Ts": str(datetime.datetime.now()),
                    "Sql": rendered_sql,
                    "BQ_Job_Id": query_job.job_id,
                    "Dry_Run": query_job.dry_run
                }]
            )

            if rows == 0:
                break

            last_success_key = str(int(last_success_key) + rows)

            write_checkpoint(
                bq_client=bq_client,
                job_name=job_name,
                run_id=run_id,
                batch_no=batch_no,
                last_success_key=last_success_key,
                rows_processed=rows,
                status="SUCCESS"
            )

            batch_no += 1

        except Exception as e:
            write_checkpoint(
                bq_client=bq_client,
                job_name=job_name,
                run_id=run_id,
                batch_no=batch_no,
                last_success_key=last_success_key,
                rows_processed=0,
                status="FAILED",
                error_message=str(e)
            )
            raise

EXECUTE_SQL = PythonOperator(
    task_id="EXECUTE_SQL",
    python_callable=execute_sql,
    dag=dag
)
