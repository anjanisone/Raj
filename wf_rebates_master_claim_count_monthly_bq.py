import airflow
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import storage, bigquery
import subprocess
import time
import sys

ENV = Variable.get("ENV")
GCP_PROJECT = Variable.get("GCP_PROJECT")
COMPOSER_GS_BUCKET = Variable.get("GCS_BUCKET")

DAG_NAME = "wf_rebates_master_claim_count_monthly_bq"
APP_NM = "REBATES"
PRCS_NAME = "CLAIM_COUNT_DETAILS_BQ_LOAD"

SQL_FILE_PATH = (
    f"gs://{COMPOSER_GS_BUCKET}/dags/rebates/sql/"
    f"rebates_master_claim_count/bq_rebates_master_claim_count_incremental.sql"
)

TRIGGER_FILE_PATH = f"gs://bkt-{ENV}-edap-datalake-rebates-primeone/trigger/CCD/"
ARCHIVE_PATH = f"gs://bkt-{ENV}-edap-datalake-rebates-primeone/trigger/archive/"

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1,
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
)

def list_files(gcs_path, identifier):
    client = storage.Client()
    bucket_name = gcs_path.split("//")[1].split("/")[0]
    prefix = gcs_path.split(bucket_name + "/")[1]
    files = []
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        if identifier in blob.name:
            files.append(blob.name.split("/")[-1])
    return files

def run_cmd(command):
    process = subprocess.Popen(
        command, shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise Exception(stderr)
    return stdout

def sense_trigger_file(**kwargs):
    files = list_files(TRIGGER_FILE_PATH, "PO_CCD")
    if len(files) == 1:
        return "REBATES_CLAIM_COUNT_EXECUTE_SQL"
    return "PRCS_COMPLETE"

def run_bq_sql(**kwargs):
    client = bigquery.Client(project=GCP_PROJECT)
    storage_client = storage.Client()
    bucket = storage_client.bucket(SQL_FILE_PATH.split("//")[1].split("/")[0])
    blob_name = "/".join(SQL_FILE_PATH.split("/")[3:])
    sql = bucket.blob(blob_name).download_as_text()
    job = client.query(sql)
    job.result()

def archive_trigger_file(**kwargs):
    files = list_files(TRIGGER_FILE_PATH, "PO_CCD")
    if files:
        cmd = f"gsutil -m mv {TRIGGER_FILE_PATH}{files[0]} {ARCHIVE_PATH}"
        run_cmd(cmd)

def create_servicenow_incident(context):
    pass

def check_upstream_dags():
    hook = PostgresHook("airflow_db")
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("""
        select count(1)
        from dag_run
        where dag_id in (
            'wf_rebates_dst_po_pharmacy_claim_enrichment_ins_bq',
            'wf_rebates_dst_po_pharmacy_claim_transaction_ins_bq'
        )
        and state = 'running'
    """)
    running = cur.fetchone()[0]
    return running == 0

SENSE_TRIGGER_FILE = BranchPythonOperator(
    task_id="SENSE_TRIGGER_FILE",
    python_callable=sense_trigger_file,
    provide_context=True,
    dag=dag,
)

CHECK_UPSTREAM = PythonOperator(
    task_id="CHECK_UPSTREAM",
    python_callable=check_upstream_dags,
    dag=dag,
)

REBATES_CLAIM_COUNT_EXECUTE_SQL = PythonOperator(
    task_id="REBATES_CLAIM_COUNT_EXECUTE_SQL",
    python_callable=run_bq_sql,
    provide_context=True,
    on_failure_callback=create_servicenow_incident,
    dag=dag,
)

ARCHIVE_TRIGGER = PythonOperator(
    task_id="ARCHIVE_TRIGGER",
    python_callable=archive_trigger_file,
    provide_context=True,
    dag=dag,
)

PRCS_COMPLETE = DummyOperator(
    task_id="PRCS_COMPLETE",
    dag=dag,
)

SENSE_TRIGGER_FILE >> CHECK_UPSTREAM >> REBATES_CLAIM_COUNT_EXECUTE_SQL
REBATES_CLAIM_COUNT_EXECUTE_SQL >> ARCHIVE_TRIGGER >> PRCS_COMPLETE
SENSE_TRIGGER_FILE >> PRCS_COMPLETE