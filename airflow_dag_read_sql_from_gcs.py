import airflow
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage, bigquery

DAG_NAME = "wf_gcs_sql_to_bigquery_multitask"

GCP_PROJECT = Variable.get("GCP_PROJECT")
COMPOSER_GCS_BUCKET = Variable.get("GCS_BUCKET")

SQL_FILE_PATH = f"gs://{COMPOSER_GCS_BUCKET}/dags/sql/load_claim_count.sql"

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1,
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def read_sql_from_gcs(**context):
    storage_client = storage.Client()
    bucket = SQL_FILE_PATH.split("//")[1].split("/")[0]
    blob_path = "/".join(SQL_FILE_PATH.split("/")[3:])
    sql = storage_client.bucket(bucket).blob(blob_path).download_as_text()
    context["ti"].xcom_push(key="sql_text", value=sql)

def execute_bigquery_sql(**context):
    sql = context["ti"].xcom_pull(task_ids="READ_SQL", key="sql_text")
    client = bigquery.Client(project=GCP_PROJECT)
    job = client.query(sql)
    job.result()

READ_SQL = PythonOperator(
    task_id="READ_SQL",
    python_callable=read_sql_from_gcs,
    provide_context=True,
    dag=dag,
)

EXECUTE_SQL = PythonOperator(
    task_id="EXECUTE_SQL",
    python_callable=execute_bigquery_sql,
    provide_context=True,
    dag=dag,
)

READ_SQL >> EXECUTE_SQL